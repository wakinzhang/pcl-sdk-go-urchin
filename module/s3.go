package module

const (
	DefaultS3UploadMultiSize = 500 * 1024 * 1024

	DefaultS3UploadFileTaskNum    = 100
	DefaultS3UploadMultiTaskNum   = 20
	DefaultS3DownloadFileTaskNum  = 100
	DefaultS3DownloadMultiTaskNum = 20
)

type S3MkdirInput struct {
	ObjectKey string
}

type S3UploadInput struct {
	SourcePath string
	TargetPath string
	NeedPure   bool
}

type S3DownloadInput struct {
	SourcePath string
	TargetPath string
}

type S3DeleteInput struct {
	Path string
}
