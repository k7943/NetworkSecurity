import sys
import os
import numpy as np
import pandas as pd
from sklearn.impute import KNNImputer
from sklearn.pipeline import Pipeline

from networksecurity.constant.training_pipeline import TARGET_COLUMN
from networksecurity.constant.training_pipeline import DATA_TRANSFORMATION_IMPUTER_PARAMS
from networksecurity.entity.artifact_entity import DataTransformationArtifact, DataValidationArtifact
from networksecurity.entity.config_entity import DataTransformationConfig
from networksecurity.exception.exception import NetworkSecurityException 
from networksecurity.logging.logger import logging
from networksecurity.utils.main_utils.utils import save_numpy_array_data,save_object

class DataTransformation:
    def __init__(self,data_validation_artifact:DataValidationArtifact,data_transformation_config:DataTransformationConfig):
        try:
            self.data_validation_artifact = data_validation_artifact
            self.data_transformation_config = data_transformation_config
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def get_data_transformer_object(cls)->Pipeline:
        # It initialises a KNNImputer object with the parameters specified in the training_pipeline.py file
        # and returns a Pipeline object with the KNNImputer object as the first step.

        logging.info("Entered get_data_trnasformer_object method of Trnasformation class")
        try:
           imputer=KNNImputer(**DATA_TRANSFORMATION_IMPUTER_PARAMS)
           logging.info(f"Initialise KNNImputer with {DATA_TRANSFORMATION_IMPUTER_PARAMS}")
           processor=Pipeline([("imputer",imputer)])
           return processor
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def initiate_data_transformation(self) -> DataTransformationArtifact:
        logging.info("Entered initiate_data_transformation method of DataTransformation class")
        try:
            logging.info("Starting data transformation process")

            # using valid data because data validation process returns valid data in case of this project and dataset as explained in the data_validation.py file
            train_df = pd.read_csv(self.data_validation_artifact.valid_train_file_path)
            test_df = pd.read_csv(self.data_validation_artifact.valid_test_file_path)

            # training dataframe
            input_feature_train_df = train_df.drop(columns=[TARGET_COLUMN], axis=1)
            target_feature_train_df = train_df[TARGET_COLUMN]

            # testing dataframe
            input_feature_test_df = test_df.drop(columns=[TARGET_COLUMN], axis=1)
            target_feature_test_df = test_df[TARGET_COLUMN]

            # Imputation using KNN
            imputer = KNNImputer(n_neighbors=DATA_TRANSFORMATION_IMPUTER_PARAMS["n_neighbors"])

            # Though there is no requirement for doing preprocessing for the current dataset as there are no nan values
            # but still we are doing it to make the code more generic and reusable for other datasets.
            preprocessor=self.get_data_transformer_object()

            preprocessor_object=preprocessor.fit(input_feature_train_df)
            transformed_input_train_feature=preprocessor_object.transform(input_feature_train_df)
            transformed_input_test_feature =preprocessor_object.transform(input_feature_test_df)

            train_arr = np.c_[transformed_input_train_feature, np.array(target_feature_train_df) ]
            test_arr = np.c_[ transformed_input_test_feature, np.array(target_feature_test_df) ]

            #save numpy array data
            save_numpy_array_data( self.data_transformation_config.transformed_train_file_path, array=train_arr)
            save_numpy_array_data( self.data_transformation_config.transformed_test_file_path,array=test_arr)
            save_object( self.data_transformation_config.transformed_object_file_path, preprocessor_object)

            #preparing artifacts
            data_transformation_artifact=DataTransformationArtifact(
                transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
                transformed_train_file_path=self.data_transformation_config.transformed_train_file_path,
                transformed_test_file_path=self.data_transformation_config.transformed_test_file_path
            )
            return data_transformation_artifact



        except Exception as e:
            raise NetworkSecurityException(e, sys)
