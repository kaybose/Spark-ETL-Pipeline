from pyspark.sql.functions import monotonically_increasing_id, split
from pycountry import countries
from collections import defaultdict
from pyspark.sql.types import StructType, StructField, StringType


def transform_data(spark, df_projects, df_code):
    print("Transforming...")


    # removing first three rows which are not relevant
    df1 = df_projects.withColumn("Index", monotonically_increasing_id())
    df_dropped = df1.select('pid','regionname','countryname','boardapprovaldate','closingdate','curr_project_cost','curr_ibrd_commitment','curr_ida_commitment','curr_total_commit','grantamt'). \
        filter('index > 2'). \
        drop("Index")

    df_ready = df_dropped.filter("countryname IS NOT NULL"). \
        withColumn("boardapprovaldate", split("boardapprovaldate", "T")[0]). \
        withColumn("closingdate", split("closingdate", "T")[0])


    # from collections import defaultdict
    country_not_found = []  # stores countries not found in the pycountry library
    country_namecode_dict = defaultdict(str)  # set up an empty dictionary

    for country in df_ready.select("countryname").distinct().collect():
        try:

            country_namecode_dict[country[0]] = countries.lookup(country[0]).alpha_3
            # print(country["countryname"], 'FOUND')
        except:

            # print(country["countryname"], ' not found')
            country_not_found.append(country)

    country_not_found_mapping = {'Co-operative Republic of Guyana': 'GUY',
                                 'Commonwealth of Australia': 'AUS',
                                 'Democratic Republic of Sao Tome and Prin': 'STP',
                                 'Democratic Republic of the Congo': 'COD',
                                 'Democratic Socialist Republic of Sri Lan': 'LKA',
                                 'East Asia and Pacific': 'EAS',
                                 'Europe and Central Asia': 'ECS',
                                 'Islamic  Republic of Afghanistan': 'AFG',
                                 'Latin America': 'LCN',
                                 'Caribbean': 'LCN',
                                 'Macedonia': 'MKD',
                                 'Middle East and North Africa': 'MEA',
                                 'Oriental Republic of Uruguay': 'URY',
                                 'Republic of Congo': 'COG',
                                 "Republic of Cote d'Ivoire": 'CIV',
                                 'Republic of Korea': 'KOR',
                                 'Republic of Niger': 'NER',
                                 'Republic of Kosovo': 'XKX',
                                 'Republic of Rwanda': 'RWA',
                                 'Republic of The Gambia': 'GMB',
                                 'Republic of Togo': 'TGO',
                                 'Republic of the Union of Myanmar': 'MMR',
                                 'Republica Bolivariana de Venezuela': 'VEN',
                                 'Sint Maarten': 'SXM',
                                 "Socialist People's Libyan Arab Jamahiriy": 'LBY',
                                 'Socialist Republic of Vietnam': 'VNM',
                                 'Somali Democratic Republic': 'SOM',
                                 'South Asia': 'SAS',
                                 'St. Kitts and Nevis': 'KNA',
                                 'St. Lucia': 'LCA',
                                 'St. Vincent and the Grenadines': 'VCT',
                                 'State of Eritrea': 'ERI',
                                 'The Independent State of Papua New Guine': 'PNG',
                                 'West Bank and Gaza': 'PSE',
                                 'World': 'WLD'}

    country_namecode_dict.update(country_not_found_mapping)

    # ddf = spark.createDataFrame(data_dict, schema)
    countrycode_schema = StructType([ \
        StructField("countryname", StringType(), True), \
        StructField("countrycode", StringType(), True) \
        ])

    df_dict = spark.createDataFrame(country_namecode_dict.items(), countrycode_schema)

    # joining the dataframes on the countryname and country column to add the countrycode
    df_proj_final = df_ready.join(df_dict, df_ready.countryname == df_dict.countryname, "leftouter").drop("countryname")
    #df_proj_final.show()
    df_code_dropped = df_code.select("Country Code", "IncomeGroup").drop("SpecialNotes", "_c5")
    df_final = df_proj_final.join(df_code_dropped, df_proj_final["countrycode"] == df_code_dropped["Country Code"], "leftouter")
    #df_final.show()

    return df_final
