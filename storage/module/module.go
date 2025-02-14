package module

import (
	"encoding/xml"
)

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
	Source *string `protobuf:"bytes,2,opt,name=source,proto3,oneof" json:"source"`
}

type CreateGetObjectMetadataSignedUrlReq struct {
	// @inject_tag: json:"task_id"
	TaskId int32 `protobuf:"varint,1,opt,name=task_id,proto3" json:"task_id"`
	// @inject_tag: json:"source"
	Source string `protobuf:"bytes,2,opt,name=source,proto3" json:"source"`
}

type CreateGetObjectSignedUrlReq struct {
	// @inject_tag: json:"range_start"
	RangeStart int64 `protobuf:"varint,1,opt,name=range_start,proto3" json:"range_start"`
	// @inject_tag: json:"range_end"
	RangeEnd int64 `protobuf:"varint,2,opt,name=range_end,proto3" json:"range_end"`
	// @inject_tag: json:"task_id"
	TaskId int32 `protobuf:"varint,3,opt,name=task_id,proto3" json:"task_id"`
	// @inject_tag: json:"source"
	Source string `protobuf:"bytes,4,opt,name=source,proto3" json:"source"`
}

type CreateListObjectsSignedUrlReq struct {
	// @inject_tag: json:"task_id"
	TaskId int32 `protobuf:"varint,1,opt,name=task_id,proto3" json:"task_id"`
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

type GetIpfsTokenReq struct {
	// @inject_tag: json:"node_name"
	NodeName string `protobuf:"bytes,1,opt,name=node_name,proto3" json:"node_name"`
}

type GetIpfsTokenResp struct {
	// @inject_tag: json:"code"
	Code int32 `protobuf:"varint,1,opt,name=code,proto3" json:"code"`
	// @inject_tag: json:"message"
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message"`
	// @inject_tag: json:"url"
	Url string `protobuf:"bytes,3,opt,name=url,proto3" json:"url"`
	// @inject_tag: json:"token"
	Token string `protobuf:"bytes,4,opt,name=token,proto3" json:"token"`
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
	// @inject_tag: json:"node_name"
	NodeName string `protobuf:"bytes,5,opt,name=node_name,proto3" json:"node_name"`
}

type GetObjectReq struct {
	// @inject_tag: json:"user_id"
	UserId string `protobuf:"bytes,1,opt,name=user_id,proto3" json:"user_id"`
	// @inject_tag: json:"page_index"
	PageIndex int32 `protobuf:"varint,2,opt,name=page_index,proto3" json:"page_index"`
	// @inject_tag: json:"page_size"
	PageSize int32 `protobuf:"varint,3,opt,name=page_size,proto3" json:"page_size"`
	// @inject_tag: json:"sort_by"
	SortBy *string `protobuf:"bytes,4,opt,name=sort_by,proto3,oneof" json:"sort_by"`
	// @inject_tag: json:"order_by"
	OrderBy *string `protobuf:"bytes,5,opt,name=order_by,proto3,oneof" json:"order_by"`
	// @inject_tag: json:"obj_uuid"
	ObjUuid *string `protobuf:"bytes,6,opt,name=obj_uuid,proto3,oneof" json:"obj_uuid"`
}

type GetObjectResp struct {
	// @inject_tag: json:"code"
	Code int32 `protobuf:"varint,1,opt,name=code,proto3" json:"code"`
	// @inject_tag: json:"message"
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message"`
	// @inject_tag: json:"data"
	Data *GetObjectRespData `protobuf:"bytes,3,opt,name=data,proto3" json:"data"`
}

type GetObjectRespData struct {
	// @inject_tag: json:"total"
	Total int32 `protobuf:"varint,1,opt,name=total,proto3" json:"total"`
	// @inject_tag: json:"list"
	List []*DataObj `protobuf:"bytes,2,rep,name=list,proto3" json:"list"`
}

type DataObj struct {
	// @inject_tag: json:"id" xorm:"pk autoincr"
	Id int32 `protobuf:"varint,1,opt,name=id,proto3" json:"id" xorm:"pk autoincr"`
	// @inject_tag: json:"uuid" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('数据对象唯一标识')"
	Uuid string `protobuf:"bytes,2,opt,name=uuid,proto3" json:"uuid" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('数据对象唯一标识')"`
	// @inject_tag: json:"name" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('数据对象名称')"
	Name string `protobuf:"bytes,3,opt,name=name,proto3" json:"name" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('数据对象名称')"`
	// @inject_tag: json:"type" xorm:"INT notnull default(0) comment('数据对象类型，1：文件；2：文件夹')"
	Type int32 `protobuf:"varint,4,opt,name=type,proto3" json:"type" xorm:"INT notnull default(0) comment('数据对象类型，1：文件；2：文件夹')"`
	// @inject_tag: json:"desc" xorm:"TEXT comment('数据对象描述信息')"
	Desc string `protobuf:"bytes,5,opt,name=desc,proto3" json:"desc" xorm:"TEXT comment('数据对象描述信息')"`
	// @inject_tag: json:"size" xorm:"INT notnull default(0) comment('数据对象大小，单位字节')"
	Size int32 `protobuf:"varint,6,opt,name=size,proto3" json:"size" xorm:"INT notnull default(0) comment('数据对象大小，单位字节')"`
	// @inject_tag: json:"status" xorm:"INT notnull default(0) comment('数据对象状态，0：初始状态；1：上传中；2：复制中；3：正常状态；4：操作中；5：已删除；6：上传失败；7：复制失败')"
	Status int32 `protobuf:"varint,7,opt,name=status,proto3" json:"status" xorm:"INT notnull default(0) comment('数据对象状态，0：初始状态；1：上传中；2：复制中；3：正常状态；4：操作中；5：已删除；6：上传失败；7：复制失败')"`
	// @inject_tag: json:"user_id" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('数据对象关联用户id')"
	UserId string `protobuf:"bytes,8,opt,name=user_id,proto3" json:"user_id" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('数据对象关联用户id')"`
	// @inject_tag: json:"version" xorm:"version BIGINT notnull default(0) comment('版本控制')"
	Version int32 `protobuf:"varint,9,opt,name=version,proto3" json:"version" xorm:"version BIGINT notnull default(0) comment('版本控制')"`
	// @inject_tag: json:"create_time" xorm:"TIMESTAMP notnull created comment('记录创建时间')"
	CreateTime string `protobuf:"bytes,10,opt,name=create_time,proto3" json:"create_time" xorm:"TIMESTAMP notnull created comment('记录创建时间')"`
	// @inject_tag: json:"update_time" xorm:"TIMESTAMP notnull updated comment('记录变更时间')"
	UpdateTime string `protobuf:"bytes,11,opt,name=update_time,proto3" json:"update_time" xorm:"TIMESTAMP notnull updated comment('记录变更时间')"`
	// @inject_tag: json:"delete_time" xorm:"TIMESTAMP NOT NULL DEFAULT '1970-01-01 08:00:01' comment('记录删除时间')"
	DeleteTime string `protobuf:"bytes,12,opt,name=delete_time,proto3" json:"delete_time" xorm:"TIMESTAMP NOT NULL DEFAULT '1970-01-01 08:00:01' comment('记录删除时间')"`
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
	// @inject_tag: json:"node_type"
	NodeType int32 `protobuf:"varint,4,opt,name=node_type,proto3" json:"node_type"`
	// @inject_tag: json:"bucket_name"
	BucketName string `protobuf:"bytes,5,opt,name=bucket_name,proto3" json:"bucket_name"`
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
	// @inject_tag: json:"source_node_type"
	SourceNodeType int32 `protobuf:"varint,4,opt,name=source_node_type,proto3" json:"source_node_type"`
	// @inject_tag: json:"source_bucket_name"
	SourceBucketName string `protobuf:"bytes,5,opt,name=source_bucket_name,proto3" json:"source_bucket_name"`
	// @inject_tag: json:"target_node_type"
	TargetNodeType int32 `protobuf:"varint,6,opt,name=target_node_type,proto3" json:"target_node_type"`
	// @inject_tag: json:"data_object_type"
	DataObjectType int32 `protobuf:"varint,7,opt,name=data_object_type,proto3" json:"data_object_type"`
	// @inject_tag: json:"location"
	Location string `protobuf:"bytes,8,opt,name=location,proto3" json:"location"`
}

type PutObjectDeploymentReq struct {
	// @inject_tag: json:"obj_uuid"
	ObjUuid string `protobuf:"bytes,1,opt,name=obj_uuid,proto3" json:"obj_uuid"`
	// @inject_tag: json:"node_name"
	NodeName string `protobuf:"bytes,2,opt,name=node_name,proto3" json:"node_name"`
	// @inject_tag: json:"location"
	Location *string `protobuf:"bytes,3,opt,name=location,proto3,oneof" json:"location"`
}

type GetTaskReq struct {
	// @inject_tag: json:"page_index"
	PageIndex int32 `protobuf:"varint,1,opt,name=page_index,proto3" json:"page_index"`
	// @inject_tag: json:"page_size"
	PageSize int32 `protobuf:"varint,2,opt,name=page_size,proto3" json:"page_size"`
	// @inject_tag: json:"sort_by"
	SortBy *string `protobuf:"bytes,3,opt,name=sort_by,proto3,oneof" json:"sort_by"`
	// @inject_tag: json:"order_by"
	OrderBy *string `protobuf:"bytes,4,opt,name=order_by,proto3,oneof" json:"order_by"`
	// @inject_tag: json:"task_id"
	TaskId *int32 `protobuf:"varint,5,opt,name=task_id,proto3,oneof" json:"task_id"`
}

type GetTaskResp struct {
	// @inject_tag: json:"code"
	Code int32 `protobuf:"varint,1,opt,name=code,proto3" json:"code"`
	// @inject_tag: json:"message"
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message"`
	// @inject_tag: json:"data"
	Data *GetTaskRespData `protobuf:"bytes,3,opt,name=data,proto3" json:"data"`
}

type GetTaskRespData struct {
	// @inject_tag: json:"total"
	Total int32 `protobuf:"varint,1,opt,name=total,proto3" json:"total"`
	// @inject_tag: json:"list"
	List []*TaskDetail `protobuf:"bytes,2,rep,name=list,proto3" json:"list"`
}

type TaskDetail struct {
	// @inject_tag: json:"task"
	Task *Task `protobuf:"bytes,1,opt,name=task,proto3" json:"task"`
	// @inject_tag: json:"execs"
	Execs []*TaskExec `protobuf:"bytes,2,rep,name=execs,proto3" json:"execs"`
}

type Task struct {
	// @inject_tag: json:"id" xorm:"pk autoincr"
	Id int32 `protobuf:"varint,1,opt,name=id,proto3" json:"id" xorm:"pk autoincr"`
	// @inject_tag: json:"type" xorm:"INT notnull default(0) comment('任务类型，1：上传；2：下载；3：迁移；4：删除')"
	Type int32 `protobuf:"varint,2,opt,name=type,proto3" json:"type" xorm:"INT notnull default(0) comment('任务类型，1：上传；2：下载；3：迁移；4：删除')"`
	// @inject_tag: json:"name" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('任务名称')"
	Name string `protobuf:"bytes,3,opt,name=name,proto3" json:"name" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('任务名称')"`
	// @inject_tag: json:"params" xorm:"TEXT comment('任务参数')"
	Params string `protobuf:"bytes,4,opt,name=params,proto3" json:"params" xorm:"TEXT comment('任务参数')"`
	// @inject_tag: json:"status" xorm:"INT notnull default(0) comment('任务状态，0：初始状态；1：执行中；2：执行完成；3：取消')"
	Status int32 `protobuf:"varint,5,opt,name=status,proto3" json:"status" xorm:"INT notnull default(0) comment('任务状态，0：初始状态；1：执行中；2：执行完成；3：取消')"`
	// @inject_tag: json:"task_exec_id" xorm:"INT notnull default(0) comment('任务执行记录id')"
	TaskExecId int32 `protobuf:"varint,6,opt,name=task_exec_id,proto3" json:"task_exec_id" xorm:"INT notnull default(0) comment('任务执行记录id')"`
	// @inject_tag: json:"result" xorm:"INT notnull default(0) comment('任务执行结果，0：初始状态；1：成功；2：失败')"
	Result int32 `protobuf:"varint,7,opt,name=result,proto3" json:"result" xorm:"INT notnull default(0) comment('任务执行结果，0：初始状态；1：成功；2：失败')"`
	// @inject_tag: json:"return" xorm:"TEXT comment('任务返回信息')"
	Return string `protobuf:"bytes,8,opt,name=return,proto3" json:"return" xorm:"TEXT comment('任务返回信息')"`
	// @inject_tag: json:"obj_uuid" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('数据对象唯一标识')"
	ObjUuid string `protobuf:"bytes,9,opt,name=obj_uuid,proto3" json:"obj_uuid" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('数据对象唯一标识')"`
	// @inject_tag: json:"user_id" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('任务关联用户id')"
	UserId string `protobuf:"bytes,10,opt,name=user_id,proto3" json:"user_id" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('任务关联用户id')"`
	// @inject_tag: json:"start_time" xorm:"TIMESTAMP NOT NULL DEFAULT '1970-01-01 08:00:01' comment('任务开始时间')"
	StartTime string `protobuf:"bytes,11,opt,name=start_time,proto3" json:"start_time" xorm:"TIMESTAMP NOT NULL DEFAULT '1970-01-01 08:00:01' comment('任务开始时间')"`
	// @inject_tag: json:"finish_time" xorm:"TIMESTAMP NOT NULL DEFAULT '1970-01-01 08:00:01' comment('任务结束时间')"
	FinishTime string `protobuf:"bytes,12,opt,name=finish_time,proto3" json:"finish_time" xorm:"TIMESTAMP NOT NULL DEFAULT '1970-01-01 08:00:01' comment('任务结束时间')"`
	// @inject_tag: json:"create_time" xorm:"TIMESTAMP notnull created comment('记录创建时间')"
	CreateTime string `protobuf:"bytes,13,opt,name=create_time,proto3" json:"create_time" xorm:"TIMESTAMP notnull created comment('记录创建时间')"`
	// @inject_tag: json:"update_time" xorm:"TIMESTAMP notnull updated comment('记录变更时间')"
	UpdateTime string `protobuf:"bytes,14,opt,name=update_time,proto3" json:"update_time" xorm:"TIMESTAMP notnull updated comment('记录变更时间')"`
}

type UploadObjectTaskParams struct {
	// @inject_tag: json:"request"
	Request *UploadObjectReq `protobuf:"bytes,1,opt,name=request,proto3" json:"request"`
	// @inject_tag: json:"uuid"
	Uuid string `protobuf:"bytes,2,opt,name=uuid,proto3" json:"uuid"`
	// @inject_tag: json:"node_id"
	NodeId int32 `protobuf:"varint,3,opt,name=node_id,proto3" json:"node_id"`
	// @inject_tag: json:"node_name"
	NodeName string `protobuf:"bytes,4,opt,name=node_name,proto3" json:"node_name"`
	// @inject_tag: json:"location"
	Location string `protobuf:"bytes,5,opt,name=location,proto3" json:"location"`
}

type DownloadObjectTaskParams struct {
	// @inject_tag: json:"request"
	Request *DownloadObjectReq `protobuf:"bytes,1,opt,name=request,proto3" json:"request"`
	// @inject_tag: json:"node_id"
	NodeId int32 `protobuf:"varint,2,opt,name=node_id,proto3" json:"node_id"`
	// @inject_tag: json:"node_name"
	NodeName string `protobuf:"bytes,3,opt,name=node_name,proto3" json:"node_name"`
	// @inject_tag: json:"location"
	Location string `protobuf:"bytes,4,opt,name=location,proto3" json:"location"`
}

type TaskExec struct {
	// @inject_tag: json:"id" xorm:"pk autoincr"
	Id int32 `protobuf:"varint,1,opt,name=id,proto3" json:"id" xorm:"pk autoincr"`
	// @inject_tag: json:"task_id" xorm:"INT notnull default(0) comment('任务id')"
	TaskId int32 `protobuf:"varint,2,opt,name=task_id,proto3" json:"task_id" xorm:"INT notnull default(0) comment('任务id')"`
	// @inject_tag: json:"context" xorm:"TEXT comment('执行上下文')"
	Context string `protobuf:"bytes,3,opt,name=context,proto3" json:"context" xorm:"TEXT comment('执行上下文')"`
	// @inject_tag: json:"status" xorm:"INT notnull default(0) comment('执行状态，0：初始状态；1：执行中；2：执行完成；3：取消')"
	Status int32 `protobuf:"varint,4,opt,name=status,proto3" json:"status" xorm:"INT notnull default(0) comment('执行状态，0：初始状态；1：执行中；2：执行完成；3：取消')"`
	// @inject_tag: json:"result" xorm:"INT notnull default(0) comment('执行结果，0：初始状态；1：成功；2：失败')"
	Result int32 `protobuf:"varint,5,opt,name=result,proto3" json:"result" xorm:"INT notnull default(0) comment('执行结果，0：初始状态；1：成功；2：失败')"`
	// @inject_tag: json:"return" xorm:"TEXT comment('执行返回信息')"
	Return string `protobuf:"bytes,6,opt,name=return,proto3" json:"return" xorm:"TEXT comment('执行返回信息')"`
	// @inject_tag: json:"start_time" xorm:"TIMESTAMP NOT NULL DEFAULT '1970-01-01 08:00:01' comment('任务开始时间')"
	StartTime string `protobuf:"bytes,7,opt,name=start_time,proto3" json:"start_time" xorm:"TIMESTAMP NOT NULL DEFAULT '1970-01-01 08:00:01' comment('任务开始时间')"`
	// @inject_tag: json:"finish_time" xorm:"TIMESTAMP NOT NULL DEFAULT '1970-01-01 08:00:01' comment('任务结束时间')"
	FinishTime string `protobuf:"bytes,8,opt,name=finish_time,proto3" json:"finish_time" xorm:"TIMESTAMP NOT NULL DEFAULT '1970-01-01 08:00:01' comment('任务结束时间')"`
	// @inject_tag: json:"create_time" xorm:"TIMESTAMP notnull created comment('记录创建时间')"
	CreateTime string `protobuf:"bytes,9,opt,name=create_time,proto3" json:"create_time" xorm:"TIMESTAMP notnull created comment('记录创建时间')"`
	// @inject_tag: json:"update_time" xorm:"TIMESTAMP notnull updated comment('记录变更时间')"
	UpdateTime string `protobuf:"bytes,10,opt,name=update_time,proto3" json:"update_time" xorm:"TIMESTAMP notnull updated comment('记录变更时间')"`
}

type FinishTaskReq struct {
	// @inject_tag: json:"task_id"
	TaskId int32 `protobuf:"varint,1,opt,name=task_id,proto3" json:"task_id"`
	// @inject_tag: json:"result"
	Result int32 `protobuf:"varint,2,opt,name=result,proto3" json:"result"`
	// @inject_tag: json:"return"
	Return *string `protobuf:"bytes,3,opt,name=return,proto3,oneof" json:"return"`
}

type XIpfsUpload struct {
	XMLName xml.Name `xml:"IpfsUpload"`
	CId     string   `xml:"CId"`
	Result  int      `xml:"Result"`
}

type XIpfsDownload struct {
	XMLName xml.Name `xml:"IpfsDownload"`
	Result  int      `xml:"Result"`
}
