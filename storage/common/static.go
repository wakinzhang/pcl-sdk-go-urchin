package common

const (
	StorageCategoryEIpfs  = 1
	StorageCategoryEObs   = 2
	StorageCategoryEMinio = 3

	UrchinServiceCreateInitiateMultipartUploadSignedUrlInterface = "/v1/object/auth/create_init_multi_part_upload_signed_url"
	UrchinServiceCreateUploadPartSignedUrlInterface              = "/v1/object/auth/create_upload_part_signed_url"
	UrchinServiceCreateCompleteMultipartUploadSignedUrlInterface = "/v1/object/auth/create_complete_multi_part_upload_signed_url"
	UrchinServiceCreateNewFolderSignedUrlInterface               = "/v1/object/auth/create_new_folder_signed_url"
	UrchinServiceCreateGetObjectMetadataSignedUrlInterface       = "/v1/object/auth/create_get_object_metadata_signed_url"
	UrchinServiceCreateGetObjectSignedUrlInterface               = "/v1/object/auth/create_get_object_signed_url"
	UrchinServiceCreateListObjectsSignedUrlInterface             = "/v1/object/auth/create_list_objects_signed_url"
	UrchinServiceGetIpfsTokenInterface                           = "/v1/object/auth/get_ipfs_token"

	UrchinServiceUploadObjectInterface        = "/v1/object/upload"
	UrchinServiceDownloadObjectInterface      = "/v1/object/download"
	UrchinServiceMigrateObjectInterface       = "/v1/object/migrate"
	UrchinServiceGetObjectInterface           = "/v1/object"
	UrchinServicePutObjectDeploymentInterface = "/v1/object/deployment"

	UrchinServiceUploadFileInterface   = "/v1/object/file/upload"
	UrchinServiceDownloadFileInterface = "/v1/object/file/download"

	UrchinServiceGetTaskInterface    = "/v1/task/get"
	UrchinServiceFinishTaskInterface = "/v1/task/finish"

	DataObjectTypeEFile   = 1
	DataObjectTypeEFolder = 2

	TaskFResultESuccess = 1
	TaskFResultEFailed  = 2

	DefaultPartSize            = 100 * 1024 * 1024
	DefaultUploadMultiNumber   = 5
	DefaultDownloadFileTaskNum = 3

	DefaultPageIndex = 1
	DefaultPageSize  = 10

	HttpMethodGet  = "GET"
	HttpMethodPost = "Post"
	HttpMethodPut  = "Put"

	ChanResultSuccess = 0
	ChanResultFailed  = -1

	TaskTypeUpload       = 1
	TaskTypeDownload     = 2
	TaskTypeMigrate      = 3
	TaskTypeCopy         = 4
	TaskTypeUploadFile   = 5
	TaskTypeDownloadFile = 6
)
