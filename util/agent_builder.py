from google.cloud import discoveryengine_v1alpha as discoveryengine
from google.api_core.client_options import ClientOptions
from typing import Optional, Any, Literal
from google.protobuf.json_format import MessageToDict
import logging

logger = logging.getLogger(__name__)

class AgentBuilderUtil:
    def __init__(self, project_id: str, location: str, collection_id: str = "default_collection"):
        self.project_id = project_id
        self.location = location
        self.collection_id = collection_id
        self.client_options = (
            ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
            if location != "global"
            else None
        )
        self.datastore_client = discoveryengine.DataStoreServiceClient(client_options=self.client_options)
        self.document_client = discoveryengine.DocumentServiceClient(client_options=self.client_options)
        self.engine_client = discoveryengine.EngineServiceClient(client_options=self.client_options)
        self.chunk_client = discoveryengine.ChunkServiceClient(client_options=self.client_options)
        self.search_client = discoveryengine.SearchServiceClient(client_options=self.client_options)



    def generate_document_processing_config(
        self,
        data_store_id: str,
        parsing_config_type: str = "layout_parsing_config",
        use_native_text: bool = False,
        enable_chunking: bool = False,
        chunk_size: int = 500,
        include_ancestor_headings: bool = True,
        file_type_overrides: dict[str, dict] = None
    ) -> discoveryengine.DocumentProcessingConfig:

        parsing_config = discoveryengine.DocumentProcessingConfig.ParsingConfig()

        if parsing_config_type == "digital_parsing_config":
            parsing_config.digital_parsing_config = discoveryengine.DocumentProcessingConfig.ParsingConfig.DigitalParsingConfig()
        elif parsing_config_type == "ocr_parsing_config":
            parsing_config.ocr_parsing_config = discoveryengine.DocumentProcessingConfig.ParsingConfig.OcrParsingConfig(
                use_native_text=use_native_text
            )
        elif parsing_config_type == "layout_parsing_config":
            parsing_config.layout_parsing_config = discoveryengine.DocumentProcessingConfig.ParsingConfig.LayoutParsingConfig()
        else:
            raise ValueError("Invalid parsing_config_type.")

        document_processing_config = discoveryengine.DocumentProcessingConfig(
            name=f"projects/{self.project_id}/locations/{self.location}/collections/{self.collection_id}/dataStores/{data_store_id}/documentProcessingConfig",
            default_parsing_config=parsing_config
        )

        if enable_chunking:
            document_processing_config.chunking_config = discoveryengine.DocumentProcessingConfig.ChunkingConfig(
                layout_based_chunking_config=discoveryengine.DocumentProcessingConfig.ChunkingConfig.LayoutBasedChunkingConfig(
                    chunk_size=chunk_size,
                    include_ancestor_headings=include_ancestor_headings,
                )
            )

        if file_type_overrides:
            for file_type, overrides in file_type_overrides.items():
                if file_type not in ["pdf", "html", "docx", "pptx", "xlsm", "xlsx"]:
                    raise ValueError(f"Unsupported file type: {file_type}")

                override_config = discoveryengine.DocumentProcessingConfig.ParsingConfig()

                if "parsing_config_type" in overrides:
                    override_type = overrides["parsing_config_type"]
                    if override_type == "digital_parsing_config":
                        if file_type not in ["pdf", "html", "docx", "pptx", "xlsm", "xlsx"]:
                            raise ValueError(f"digital_parsing_config is not supported for: {file_type}")
                        override_config.digital_parsing_config = discoveryengine.DocumentProcessingConfig.ParsingConfig.DigitalParsingConfig()
                    elif override_type == "ocr_parsing_config":
                        if file_type != "pdf":
                           raise ValueError(f"ocr_parsing_config is only supported for PDF files.")
                        override_config.ocr_parsing_config = discoveryengine.DocumentProcessingConfig.ParsingConfig.OcrParsingConfig(
                           use_native_text = overrides.get("use_native_text", False)
                        )
                    elif override_type == "layout_parsing_config":
                        override_config.layout_parsing_config = discoveryengine.DocumentProcessingConfig.ParsingConfig.LayoutParsingConfig()
                    else:
                        raise ValueError("Invalid parsing_config_type in overrides.")

                document_processing_config.parsing_config_overrides[file_type] = override_config

        if enable_chunking and not parsing_config_type == "layout_parsing_config":
            raise ValueError("Chunking is only supported with layout_parsing_config.")

        if chunk_size < 100 or chunk_size > 500:
            raise ValueError("chunk_size must be between 100 and 500 inclusive.")

        return document_processing_config

    def create_datastore(
        self,
        data_store_id: str,
        starting_schema: discoveryengine.Schema = None,
        document_processing_config: discoveryengine.DocumentProcessingConfig = None
    ):
        request = discoveryengine.CreateDataStoreRequest(
            parent=f"projects/{self.project_id}/locations/{self.location}/collections/{self.collection_id}",
            data_store=discoveryengine.DataStore(
                name=f"projects/{self.project_id}/locations/{self.location}/collections/{self.collection_id}/dataStores/{data_store_id}",
                display_name=data_store_id,
                industry_vertical=discoveryengine.IndustryVertical.GENERIC,
                solution_types=[discoveryengine.SolutionType.SOLUTION_TYPE_SEARCH],
                content_config=discoveryengine.DataStore.ContentConfig.CONTENT_REQUIRED,
                document_processing_config=document_processing_config,
                starting_schema=starting_schema
            ),
            data_store_id=data_store_id
        )
        return self.datastore_client.create_data_store(request=request)

    def import_documents(
        self,
        data_store_id: str,
        gcs_uri: Optional[str] = None,
        bigquery_dataset: Optional[str] = None,
        bigquery_table: Optional[str] = None,
    ) -> str:
        parent = self.document_client.branch_path(
            project=self.project_id,
            location=self.location,
            data_store=data_store_id,
            branch="default_branch",
        )

        if gcs_uri:
            request = discoveryengine.ImportDocumentsRequest(
                parent=parent,
                gcs_source=discoveryengine.GcsSource(
                    input_uris=[gcs_uri], data_schema="document"
                ),
                reconciliation_mode=discoveryengine.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL,
            )
        else:
            request = discoveryengine.ImportDocumentsRequest(
                parent=parent,
                bigquery_source=discoveryengine.BigQuerySource(
                    project_id=self.project_id,
                    dataset_id=bigquery_dataset,
                    table_id=bigquery_table,
                    data_schema="custom", #If you have defined a schema, make sure to set it to "custom"
                ),
                reconciliation_mode=discoveryengine.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL,
            )

        operation = self.document_client.import_documents(request=request)
        return operation

    def list_documents(self, data_store_id: str) -> Any:
        parent = self.document_client.branch_path(
            project=self.project_id,
            location=self.location,
            data_store=data_store_id,
            branch="default_branch",
        )

        response = self.document_client.list_documents(parent=parent)
        return response

    def get_processed_document(
        self,
        document_path: str,
        processed_document_type: Literal["PARSED_DOCUMENT", "CHUNKED_DOCUMENT", "PNG_CONVERTED_DOCUMENT"]
    ) -> Any:
        response = self.document_client.get_processed_document(
            request={
                "name": document_path,
                "processed_document_type": processed_document_type,
                "processed_document_format": "JSON"
            }
        )
        return response

    def get_chunk(self, chunk_path):
        request = discoveryengine.GetChunkRequest(
            name=chunk_path
        )
        chunks_response = self.chunk_client.get_chunk(
            request=request
        )
    
        return chunks_response

    def list_chunks(self, document_path):
        request = discoveryengine.ListChunksRequest(
            parent=document_path
        )
        chunks_response = self.chunk_client.list_chunks(
            request=request
        )
    
        return chunks_response
    
    def get_document(self, document):
        request = discoveryengine.GetDocumentRequest(name=document.name)
        get_document_response = self.document_client.get_document(request=request)
        return get_document_response
    
    def check_index_status(self, document):
        try:
            get_document_response = self.get_document(document)
            message = MessageToDict(get_document_response._pb)
            index_time = message.get("indexTime")  # Use .get() to avoid KeyError
            return index_time
        except Exception as e:
            Exception(f"Error checking index status: {e}")

    def create_engine(
        self,
        data_store_ids: list[str], # Changed to List[str] to accept a list of datastore IDs.
        display_name: str,
        engine_id: str
    ):
        engine = discoveryengine.Engine(
            search_engine_config=discoveryengine.Engine.SearchEngineConfig(
                search_tier="SEARCH_TIER_ENTERPRISE",
                search_add_ons=["SEARCH_ADD_ON_LLM"]
            ),
             display_name=display_name,
            solution_type="SOLUTION_TYPE_SEARCH",
            data_store_ids=data_store_ids # Assign the list of datastore IDs directly.
        )

        request = discoveryengine.CreateEngineRequest(
            parent=f"projects/{self.project_id}/locations/{self.location}/collections/{self.collection_id}",
            engine=engine,
            engine_id=engine_id,
        )

        operation = self.engine_client.create_engine(request=request)
        logging.info("Waiting for operation to complete...")  # Corrected logging level to info.
        response = operation.result()
        return response

    def generate_search_request(
        self,
        data_store_id: str,
        query: str,
        filter: Optional[str] = None,
        page_size: Optional[int] = None,
        max_extractive_answer_count: Optional[int] = None,
        max_extractive_segment_count: Optional[int] = None,
        return_extractive_segment_score: Optional[bool] = None,
        return_snippet: Optional[bool] = None,
        num_previous_chunks: Optional[int] = None,
        num_next_chunks: Optional[int] = None,
        search_result_mode: Literal["DOCUMENTS", "CHUNKS"] = "DOCUMENTS",
        spell_correction_mode: Literal["AUTO", "SUGGESTION_ONLY"] = "SUGGESTION_ONLY"
    ):
        request = discoveryengine.SearchRequest(
            serving_config=f"projects/{self.project_id}/locations/{self.location}/collections/{self.collection_id}/dataStores/{data_store_id}/servingConfigs/default_serving_config",
            query=query,
            filter=filter,
            page_size=page_size,
        )

        extractive_content_spec = (
            discoveryengine.SearchRequest.ContentSearchSpec.ExtractiveContentSpec(
                max_extractive_answer_count=max_extractive_answer_count,
                max_extractive_segment_count=max_extractive_segment_count,
                return_extractive_segment_score=return_extractive_segment_score,
            )
        )
        
        content_search_spec = discoveryengine.SearchRequest.ContentSearchSpec(
            search_result_mode=search_result_mode,
            extractive_content_spec=extractive_content_spec
        )


        if return_snippet:
            content_search_spec.snippet_spec = discoveryengine.SearchRequest.ContentSearchSpec.SnippetSpec(
                return_snippet=True
            )

        if search_result_mode == "CHUNKS":
            content_search_spec.chunk_spec = discoveryengine.SearchRequest.ContentSearchSpec.ChunkSpec(
                num_previous_chunks=num_previous_chunks,
                num_next_chunks=num_next_chunks,
            )
        
        request.content_search_spec = content_search_spec

        if spell_correction_mode:
            request.spell_correction_spec = discoveryengine.SearchRequest.SpellCorrectionSpec(
                mode=spell_correction_mode
            )

        return request

