from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionConfig, TrainingPipelineConfig, DataValidationConfig
from networksecurity.components.data_validation import DataValidation
import sys

if __name__ == "__main__":
    try:
        traininigpipelineconfig = TrainingPipelineConfig()
        dataingestionconfig = DataIngestionConfig(traininigpipelineconfig)
        data_ingestion = DataIngestion(dataingestionconfig)

        logging.info("Initiate the data ingestion process")

        dataingestionartifact = data_ingestion.initiate_data_ingestion()

        logging.info("Data ingestion process completed successfully")

        print(dataingestionartifact)

        data_validation_config = DataValidationConfig(traininigpipelineconfig)
        data_validation = DataValidation(dataingestionartifact, data_validation_config)

        logging.info("Initiating data validation process")
        
        data_validation_artifact = data_validation.initiate_data_validation()

        logging.info("Data validation process completed successfully")

        print(data_validation_artifact)

    except Exception as e:
        raise NetworkSecurityException(e, sys)