package com.soleadify.task

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, count, desc, lit, lower, regexp_replace, when}

object Main {

  def validNumber(num: Column): Column = num.rlike("[0-9]+")
  def validCountryName(num: Column): Column = num.rlike("^[\\w\\s\\.]+$")

  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf()
    sparkConfig.setMaster("local[*]")

    val spark = SparkSession.builder().appName("Test").config(sparkConfig).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val facebookDf = spark.read
      .option("delimiter", ",").option("header", true)
      .csv("src/main/resources/facebook_dataset.csv").cache()
      .withColumn("phone", regexp_replace(col("phone"), "\\+", ""))

    val googleDf = spark.read
      .option("delimiter", ",").option("header", true)
      .csv("src/main/resources/google_dataset.csv").cache()
      .withColumn("phone", regexp_replace(col("phone"), "\\+", ""))

    val websiteDf = spark.read
      .option("delimiter", ";").option("header", true)
      .csv("src/main/resources/website_dataset.csv").cache()
      .withColumn("phone", regexp_replace(col("phone"), "\\+", ""))

    /**
    1st part:
      First fo all I decided to do some input data checks to find out how the data itself looks and how do I need to join these datasets.
    I've found that website and facebook datasets have unique values in the 'domain' column, it almost doesn't contain any null values (only 1 record in website dataset)
    and most of the records from all dataset have common values between each other for this column so I decided to join by the 'domain'.
     **/
    websiteDataChecks(facebookDf, googleDf, websiteDf)
    facebookDataChecks(facebookDf, googleDf, websiteDf)
    googleDataChecks(facebookDf, googleDf, websiteDf)

    /**
    2nd part:
      Then I needed to decide which data should have the highest priority and here are my thoughts:
      - The data from the website must be the most accurate and correct as this company owns its website and should have populated the information correctly, so it has the highest priority to me;
      - The 2nd highest priority has the facebook data as it must come from some facebook page of the company, where the information is also probably populated by the company;
      - The least accurate data in my opinion is the data from google as it seems that it is just the data provided by google search. But google dataset has the biggest amount of data
    and is not uniquely identified by the 'domain' column so I have some special logic for it which will be described later.

      So in this part I'm joining website and facebook data at first as it looks easier to do.
    For company name, category, phone, country, city and region I prefer to use the data from the website as I explained earlier, but only if it is not null and 'correct'.
    Phone and country data can be easily checked by the regex - phone must only contain numbers (I removed '+' sign) and country must contain only characters and space symbols.

    Also for the columns that represent Address (country, city, region) I have a bit different logic ('use_website_address_flag' and 'is_same_country' columns are introduced for that):
      I decided not to mix the information from different sources if they have different country name as it feels not correct to me when I see Canada and New York city as the result address,
      so when website data has some value for the country but not for the city the result for the city will still be null, even when the facebook data has some city value but the country is different

    address, email, zip_code, country_code, region_code columns are taken from the facebook dataset as those are present only there.
     **/
    val interDf = websiteDf.join(facebookDf, websiteDf("root_domain") === facebookDf("domain"), "full_outer")
      .withColumn("use_website_address_flag", when(validCountryName(websiteDf("main_country")), lit(true)).otherwise(when(validCountryName(facebookDf("country_name")), lit(false)).otherwise(lit(true))))
      .withColumn("is_same_country", lower(websiteDf("main_country")) <=> lower(facebookDf("country_name")))
      .select(
        coalesce(websiteDf("root_domain"), facebookDf("domain")).as("domain"),
        coalesce(websiteDf("legal_name"), facebookDf("name")).as("name"),
        coalesce(websiteDf("s_category"), facebookDf("categories")).as("category"),
        when(validNumber(websiteDf("phone")), websiteDf("phone")).otherwise(when(validNumber(facebookDf("phone")), facebookDf("phone")).otherwise(coalesce(websiteDf("phone"), facebookDf("phone")))).as("phone"),
        when(col("use_website_address_flag"), websiteDf("main_country")).otherwise(when(col("is_same_country"), coalesce(websiteDf("main_country"), facebookDf("country_name"))).otherwise(facebookDf("country_name"))).as("country_name"),
        when(col("use_website_address_flag"), websiteDf("main_city")).otherwise(when(col("is_same_country"), coalesce(websiteDf("main_city"), facebookDf("city"))).otherwise(facebookDf("city"))).as("city"),
        when(col("use_website_address_flag"), websiteDf("main_region")).otherwise(when(col("is_same_country"), coalesce(websiteDf("main_region"), facebookDf("region_name"))).otherwise(facebookDf("region_name"))).as("region_name"),
        facebookDf("address"),
        facebookDf("email"),
        facebookDf("zip_code"),
        facebookDf("country_code"),
        facebookDf("region_code")
      ).drop("use_website_address_flag", "is_same_country")

    /**
    3rd part:
      Finally I need to join the data from google dataset but it is a little bit trickier as 'domain' values are not unique.
     While researching how the input data looks I've got that domain of the company's website here can be specified as 'facebook.com' or 'instagram.com'
     or any other some kind of social network or aggregator website that probably means that the company doesn't have it's own website, only a page on a different resource.
      One more case for duplicate domain value is when the data contains the information about the different offices of the same company.
     So the logic here might be wrong but my decision is the following:
      - When the information from google dataset is unique for single domain ('record_count' == 1) then I'll use the data from the previous step
    and update it with the google dataset if the data is missing as google data has the least priority (same logic as above)
      - When the information from google dataset is not unique ('record_count' != 1) then I'll do the opposite - take the data from google dataset
     and update it with the data from the previous step if the data is missing. This is because either the data from google has more information about different offices of the company
     or it has some information about the company itself (in case when the domain is instagram.com the data from website and facebook datasets might contain the info
    about instagram and not about the actual company)
     */
    val updatedGoogleDf = googleDf.withColumn("record_count", count("domain").over(Window.partitionBy(col("domain"))))

     val finalDf = interDf.join(updatedGoogleDf, interDf("domain") === updatedGoogleDf("domain"), "full_outer")
       .select(
         coalesce(interDf("domain"), updatedGoogleDf("domain")).as("domain"),
         when(col("record_count") === 1, coalesce(interDf("name"), updatedGoogleDf("name"))).otherwise(coalesce(updatedGoogleDf("name"), interDf("name"))).as("name"),
         when(col("record_count") === 1, coalesce(interDf("category"), updatedGoogleDf("category"))).otherwise(coalesce(updatedGoogleDf("category"), interDf("category"))).as("category"),
         when(col("record_count") === 1, coalesce(interDf("phone"), updatedGoogleDf("phone"))).otherwise(coalesce(updatedGoogleDf("phone"), interDf("phone"))).as("phone"),
         when(col("record_count") === 1, coalesce(interDf("country_name"), updatedGoogleDf("country_name"))).otherwise(coalesce(updatedGoogleDf("country_name"), interDf("country_name"))).as("country_name"),
         when(col("record_count") === 1, coalesce(interDf("city"), updatedGoogleDf("city"))).otherwise(coalesce(updatedGoogleDf("city"), interDf("city"))).as("city"),
         when(col("record_count") === 1, coalesce(interDf("region_name"), updatedGoogleDf("region_name"))).otherwise(coalesce(updatedGoogleDf("region_name"), interDf("region_name"))).as("region_name"),
         when(col("record_count") === 1, coalesce(interDf("address"), updatedGoogleDf("address"))).otherwise(coalesce(updatedGoogleDf("address"), interDf("address"))).as("address"),
         when(col("record_count") === 1, coalesce(interDf("zip_code"), updatedGoogleDf("zip_code"))).otherwise(coalesce(updatedGoogleDf("zip_code"), interDf("zip_code"))).as("zip_code"),
         when(col("record_count") === 1, coalesce(interDf("country_code"), updatedGoogleDf("country_code"))).otherwise(coalesce(updatedGoogleDf("country_code"), interDf("country_code"))).as("country_code"),
         when(col("record_count") === 1, coalesce(interDf("region_code"), updatedGoogleDf("region_code"))).otherwise(coalesce(updatedGoogleDf("region_code"), interDf("region_code"))).as("region_code"),
         interDf("email")
       )

    //write part, the output dataset is written to the ./output/*some_name*.csv directory
    finalDf.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv("output/")
  }

  def websiteDataChecks(facebookDf: DataFrame, googleDf: DataFrame, websiteDf: DataFrame): Unit = {
    println("\nWEBSITE:")
    println("total count: " + websiteDf.count)
    println("domain distinct: " + websiteDf.select("root_domain").distinct.count)
    println("null domain: " + websiteDf.filter(col("root_domain").isNull).count)

    println("correct domain: " + websiteDf.filter(col("root_domain").rlike(".+\\..+")).count)
    println("correct country: " + websiteDf.filter(validCountryName(col("main_country"))).count)
    println("correct city: " + websiteDf.filter(validCountryName(col("main_city"))).count)
    println("correct number: " + websiteDf.filter(validNumber(col("phone"))).count)

    println("phone distinct: " + websiteDf.select("phone").distinct.count)
    println("null phone: " + websiteDf.filter(col("phone").isNull).count)

    println("legal_name distinct: " + websiteDf.select("legal_name").distinct.count)
    println("null legal_name: " + websiteDf.filter(col("legal_name").isNull).count)
    println("duplicated legal_name: " + websiteDf.groupBy("legal_name").count.filter("count > 1").count)

    println("site_name distinct: " + websiteDf.select("site_name").distinct.count)
    println("null site_name: " + websiteDf.filter(col("site_name").isNull).count)
    println("duplicated site_name: " + websiteDf.groupBy("site_name").count.filter("count > 1").count)

    println("facebookDf anti join: " + websiteDf.join(facebookDf, websiteDf("root_domain") === facebookDf("domain"), "leftanti").count)
    println("facebookDf join: " + websiteDf.join(facebookDf, websiteDf("root_domain") === facebookDf("domain")).count)

    println("googleDf anti join: " + websiteDf.join(googleDf.select("domain").distinct(), websiteDf("root_domain") === googleDf("domain"), "leftanti").count)
    println("googleDf join: " + websiteDf.join(googleDf.select("domain").distinct(), websiteDf("root_domain") === googleDf("domain")).count)

  }

  def facebookDataChecks(facebookDf: DataFrame, googleDf: DataFrame, websiteDf: DataFrame): Unit = {
    println("\nFACEBOOK:")
    println("total count: " + facebookDf.count)
    println("domain distinct: " + facebookDf.select("domain").distinct.count)
    println("null domain: " + facebookDf.filter(col("domain").isNull).count)
    println("correct domain: " + facebookDf.filter(col("domain").rlike(".+\\..+")).count)

    println("phone distinct: " + facebookDf.select("phone").distinct.count)
    println("null phone: " + facebookDf.filter(col("phone").isNull).count)

    println("name distinct: " + facebookDf.select("name").distinct.count)
    println("null name: " + facebookDf.filter(col("name").isNull).count)
    println("duplicated name: " + facebookDf.groupBy("name").count.filter("count > 1").count)


    println("websiteDf anti join: " + facebookDf.join(websiteDf, websiteDf("root_domain") === facebookDf("domain"), "leftanti").count)
    println("websiteDf join: " + facebookDf.join(websiteDf, websiteDf("root_domain") === facebookDf("domain")).count)

    println("googleDf anti join: " + facebookDf.join(googleDf.select("domain").distinct(), facebookDf("domain") === googleDf("domain"), "leftanti").count)
    println("googleDf join: " + facebookDf.join(googleDf.select("domain").distinct(), facebookDf("domain") === googleDf("domain")).count)

  }

  def googleDataChecks(facebookDf: DataFrame, googleDf: DataFrame, websiteDf: DataFrame): Unit = {
    println("\nGOOGLE:")
    println("total count: " + googleDf.count)
    println("domain distinct: " + googleDf.select("domain").distinct().count)
    println("null domain: " + googleDf.filter(col("domain").isNull).count)

    println("phone distinct: " + googleDf.select("phone").distinct().count)
    println("null phone: " + googleDf.filter(col("phone").isNull).count)

    println("name distinct: " + googleDf.select("name").distinct().count)
    println("null name: " + googleDf.filter(col("name").isNull).count)

    println("correct domain: " + googleDf.filter(col("domain").rlike(".+\\..+")).count)
    println("correct country: " + googleDf.filter(validCountryName(col("country_name"))).count)
    println("correct city: " + googleDf.filter(validCountryName(col("city"))).count)
    println("correct number: " + googleDf.filter(validNumber(col("phone"))).count)

    println("city and domain distinct: " + googleDf.select("domain", "city").distinct().count)
    println("name and domain distinct: " + googleDf.select("domain", "name").distinct().count)

    println("unique domains: " + googleDf.groupBy("domain").count.filter("count = 1").count)

    println("websiteDf join with unique domains: " + googleDf.groupBy("domain").count.filter("count = 1").join(websiteDf, websiteDf("root_domain") === googleDf("domain")).count)


    println("websiteDf anti join: " + googleDf.join(websiteDf, websiteDf("root_domain") === googleDf("domain"), "leftanti").count)
    println("websiteDf join: " + googleDf.join(websiteDf, websiteDf("root_domain") === googleDf("domain")).count)

    println("facebookDf anti join: " + googleDf.join(facebookDf, facebookDf("domain") === googleDf("domain"), "leftanti").count)
    println("facebookDf join: " + googleDf.join(facebookDf, facebookDf("domain") === googleDf("domain")).count)

    println("websiteDf name anti join: " + googleDf.join(websiteDf, lower(websiteDf("site_name")) === lower(googleDf("name")), "leftanti").count)
    println("websiteDf name join: " + googleDf.join(websiteDf, lower(websiteDf("site_name")) === lower(googleDf("name"))).count)

    googleDf.groupBy("domain").count.orderBy(desc("count")).show()
  }
}
