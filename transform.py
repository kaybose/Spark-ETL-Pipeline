from pyspark.sql.functions import monotonically_increasing_id, split
from pycountry import countries
from collections import defaultdict
from pyspark.sql.types import StructType, StructField, StringType


class Transform:
    def __init__(self, spark):
        self.spark = spark

    def transform_data(self, df):
        print("Transforming")

        #removing first three rows which are not relevant
        df1 = df.withColumn("Index", monotonically_increasing_id())
        df2 = df1.filter('index > 2').drop("Index")

        #filtering out the not null countryname
        df3 = df2.filter("countryname IS NOT NULL")

        df3. \
            withColumn("boardapprovaldate", split("boardapprovaldate", " ")[0]). \
            withColumn("closingdate", split("closingdate", " ")[0]).show()

        #from collections import defaultdict
        country_not_found = []  # stores countries not found in the pycountry library
        project_country_abbrev_dict = defaultdict(str)  # set up an empty dictionary of string values

        # iterate through the country names in df_projects.
            # Create a dictionary mapping the country name to the alpha_3 ISO code
        for country in df3.select("countryname").distinct().collect():
            try:
                # look up the country name in the pycountry library
                # store the country name as the dictionary key and the ISO-3 code as the value
                project_country_abbrev_dict[country[0]] = countries.lookup(country[0]).alpha_3
                #print(country["countryname"], 'FOUND')
            except:
                # If the country name is not in the pycountry library, then print out the country name
                # And store the results in the country_not_found list
                #print(country["countryname"], ' not found')
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

        project_country_abbrev_dict.update(country_not_found_mapping)

        # ddf = spark.createDataFrame(data_dict, schema)
        scm = StructType([\
            StructField("key", StringType(), True),\
            StructField("value", StringType(), True)\
            ])

        ddf = self.spark.createDataFrame(project_country_abbrev_dict.items(), scm)