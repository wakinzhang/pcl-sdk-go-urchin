package common

const (
	StorageCategoryEIpfs  = 1
	StorageCategoryEObs   = 2
	StorageCategoryEMinio = 3

	ConfigDefaultUrchinServiceCreateInitiateMultipartUploadSignedUrlInterface = "/v1/object/auth/create_init_multi_part_upload_signed_url"
	ConfigDefaultUrchinServiceCreateUploadPartSignedUrlInterface              = "/v1/object/auth/create_upload_part_signed_url"
	ConfigDefaultUrchinServiceCreateCompleteMultipartUploadSignedUrlInterface = "/v1/object/auth/create_complete_multi_part_upload_signed_url"
	ConfigDefaultUrchinServiceCreateNewFolderSignedUrlInterface               = "/v1/object/auth/create_new_folder_signed_url"
	ConfigDefaultUrchinServiceCreateGetObjectMetadataSignedUrlInterface       = "/v1/object/auth/create_get_object_metadata_signed_url"
	ConfigDefaultUrchinServiceCreateGetObjectSignedUrlInterface               = "/v1/object/auth/create_get_object_signed_url"
	ConfigDefaultUrchinServiceCreateListObjectsSignedUrlInterface             = "/v1/object/auth/create_list_objects_signed_url"
	ConfigDefaultUrchinServiceGetIpfsTokenInterface                           = "/v1/object/auth/get_ipfs_token"

	ConfigDefaultUrchinServiceUploadObjectInterface        = "/v1/object/upload"
	ConfigDefaultUrchinServiceDownloadObjectInterface      = "/v1/object/download"
	ConfigDefaultUrchinServiceMigrateObjectInterface       = "/v1/object/migrate"
	ConfigDefaultUrchinServiceGetObjectInterface           = "/v1/object"
	ConfigDefaultUrchinServicePutObjectDeploymentInterface = "/v1/object/deployment"

	ConfigDefaultUrchinServiceGetTaskInterface    = "/v1/task/get"
	ConfigDefaultUrchinServiceFinishTaskInterface = "/v1/task/finish"

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
)
