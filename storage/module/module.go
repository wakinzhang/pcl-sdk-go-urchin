package module

type BaseResp struct {
	// @inject_tag: json:"code"
	Code int32 `protobuf:"varint,1,opt,name=code,proto3" json:"code"`
	// @inject_tag: json:"message"
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message"`
}

type CreateInitiateMultipartUploadSignedUrlReq struct {
	// @inject_tag: json:"task_id"
	TaskId int32 `protobuf:"varint,1,opt,name=task_id,proto3" json:"task_id"`
	// @inject_tag: json:"source"
	Source string `protobuf:"bytes,2,opt,name=source,proto3" json:"source"`
}

type CreateUploadPartSignedUrlReq struct {
	// @inject_tag: json:"upload_id"
	UploadId string `protobuf:"bytes,1,opt,name=upload_id,proto3" json:"upload_id"`
	// @inject_tag: json:"part_number"
	PartNumber int32 `protobuf:"varint,2,opt,name=part_number,proto3" json:"part_number"`
	// @inject_tag: json:"task_id"
	TaskId int32 `protobuf:"varint,3,opt,name=task_id,proto3" json:"task_id"`
	// @inject_tag: json:"source"
	Source string `protobuf:"bytes,4,opt,name=source,proto3" json:"source"`
}

type CreateCompleteMultipartUploadSignedUrlReq struct {
	// @inject_tag: json:"upload_id"
	UploadId string `protobuf:"bytes,1,opt,name=upload_id,proto3" json:"upload_id"`
	// @inject_tag: json:"task_id"
	TaskId int32 `protobuf:"varint,2,opt,name=task_id,proto3" json:"task_id"`
	// @inject_tag: json:"source"
	Source string `protobuf:"bytes,3,opt,name=source,proto3" json:"source"`
}

type CreateNewFolderSignedUrlReq struct {
	// @inject_tag: json:"task_id"
	TaskId int32 `protobuf:"varint,1,opt,name=task_id,proto3" json:"task_id"`
	// @inject_tag: json:"source"
	Source string `protobuf:"bytes,2,opt,name=source,proto3" json:"source"`
}

type CreateListObjectsSignedUrlReq struct {
	// @inject_tag: json:"prefix"
	Prefix string `protobuf:"bytes,1,opt,name=prefix,proto3" json:"prefix"`
}

type CreateSignedUrlResp struct {
	// @inject_tag: json:"code"
	Code int32 `protobuf:"varint,1,opt,name=code,proto3" json:"code"`
	// @inject_tag: json:"message"
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message"`
	// @inject_tag: json:"signed_url"
	SignedUrl string `protobuf:"bytes,3,opt,name=signed_url,proto3" json:"signed_url"`
	// @inject_tag: json:"header"
	Header map[string]*HeaderValues `protobuf:"bytes,4,rep,name=header,proto3" json:"header" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

type HeaderValues struct {
	// @inject_tag: json:"values"
	Values []string `protobuf:"bytes,1,rep,name=values,proto3" json:"values"`
}

type UploadObjectReq struct {
	// @inject_tag: json:"user_id"
	UserId string `protobuf:"bytes,1,opt,name=user_id,proto3" json:"user_id"`
	// @inject_tag: json:"name"
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name"`
	// @inject_tag: json:"type"
	Type int32 `protobuf:"varint,3,opt,name=type,proto3" json:"type"`
	// @inject_tag: json:"source"
	Source string `protobuf:"bytes,4,opt,name=source,proto3" json:"source"`
	// @inject_tag: json:"size"
	Size *int32 `protobuf:"varint,5,opt,name=size,proto3,oneof" json:"size"`
	// @inject_tag: json:"desc"
	Desc *string `protobuf:"bytes,6,opt,name=desc,proto3,oneof" json:"desc"`
	// @inject_tag: json:"node_id"
	NodeId *int32 `protobuf:"varint,7,opt,name=node_id,proto3,oneof" json:"node_id"`
}

type UploadObjectResp struct {
	// @inject_tag: json:"code"
	Code int32 `protobuf:"varint,1,opt,name=code,proto3" json:"code"`
	// @inject_tag: json:"message"
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message"`
	// @inject_tag: json:"task_id"
	TaskId int32 `protobuf:"varint,3,opt,name=task_id,proto3" json:"task_id"`
	// @inject_tag: json:"node_type"
	NodeType int32 `protobuf:"varint,4,opt,name=node_type,proto3" json:"node_type"`
}

type DownloadObjectReq struct {
	// @inject_tag: json:"user_id"
	UserId string `protobuf:"bytes,1,opt,name=user_id,proto3" json:"user_id"`
	// @inject_tag: json:"obj_uuid"
	ObjUuid string `protobuf:"bytes,2,opt,name=obj_uuid,proto3" json:"obj_uuid"`
	// @inject_tag: json:"node_id"
	NodeId *int32 `protobuf:"varint,3,opt,name=node_id,proto3,oneof" json:"node_id"`
}

type DownloadObjectResp struct {
	// @inject_tag: json:"code"
	Code int32 `protobuf:"varint,1,opt,name=code,proto3" json:"code"`
	// @inject_tag: json:"message"
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message"`
	// @inject_tag: json:"task_id"
	TaskId int32 `protobuf:"varint,3,opt,name=task_id,proto3" json:"task_id"`
	// @inject_tag: json:"location"
	Location string `protobuf:"bytes,4,opt,name=location,proto3" json:"location"`
	// @inject_tag: json:"node_type"
	NodeType int32 `protobuf:"varint,5,opt,name=node_type,proto3" json:"node_type"`
}

type MigrateObjectReq struct {
	// @inject_tag: json:"user_id"
	UserId string `protobuf:"bytes,1,opt,name=user_id,proto3" json:"user_id"`
	// @inject_tag: json:"obj_uuid"
	ObjUuid string `protobuf:"bytes,2,opt,name=obj_uuid,proto3" json:"obj_uuid"`
	// @inject_tag: json:"source_node_id"
	SourceNodeId *int32 `protobuf:"varint,3,opt,name=source_node_id,proto3,oneof" json:"source_node_id"`
	// @inject_tag: json:"target_node_id"
	TargetNodeId int32 `protobuf:"varint,4,opt,name=target_node_id,proto3" json:"target_node_id"`
}

type MigrateObjectResp struct {
	// @inject_tag: json:"code"
	Code int32 `protobuf:"varint,1,opt,name=code,proto3" json:"code"`
	// @inject_tag: json:"message"
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message"`
	// @inject_tag: json:"task_id"
	TaskId int32 `protobuf:"varint,3,opt,name=task_id,proto3" json:"task_id"`
	// @inject_tag: json:"node_type"
	NodeType int32 `protobuf:"varint,4,opt,name=node_type,proto3" json:"node_type"`
}

type FinishTaskReq struct {
	// @inject_tag: json:"task_id"
	TaskId int32 `protobuf:"varint,1,opt,name=task_id,proto3" json:"task_id"`
	// @inject_tag: json:"result"
	Result int32 `protobuf:"varint,2,opt,name=result,proto3" json:"result"`
	// @inject_tag: json:"return"
	Return *string `protobuf:"bytes,3,opt,name=return,proto3,oneof" json:"return"`
}
