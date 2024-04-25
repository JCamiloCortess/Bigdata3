import unittest
from pyspark.sql import SparkSession

class TestDataProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Test Data Processing") \
            .getOrCreate()
        
        # Define datos de prueba
        cls.test_data = cls.spark.createDataFrame([(1000, 3, "some_additional_value", 50000)], ["Area", "Bedrooms", "Adicional", "Price"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_data_reading(self):
        # Prueba la lectura de datos desde S3
        df = self.spark.read.csv("s3://bucket-final/casas/year=2024/month=03/day=15/2024-03-15.csv", inferSchema=True, header=True)
        self.assertTrue(df is not None)
        df.printSchema()
        df.show()

    def test_data_preprocessing(self):
        # Prueba el preprocesamiento de datos
        preprocessed_df = preprocess_data(self.test_data)
        self.assertIsNotNone(preprocessed_df)
        preprocessed_df.printSchema()
        preprocessed_df.show()

    def test_model_creation(self):
        # Prueba la creación del modelo
        model = create_model(self.test_data)
        self.assertIsNotNone(model)

if __name__ == '__main__':
    unittest.main()
