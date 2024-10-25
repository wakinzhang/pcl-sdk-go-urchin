// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        v5.28.0
// source: module/config.proto

package module

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Config struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// @inject_tag: json:"id" xorm:"pk autoincr"
	Id int32 `protobuf:"varint,1,opt,name=id,proto3" json:"id" xorm:"pk autoincr"`
	// @inject_tag: json:"key" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('配置项名称')"
	Key string `protobuf:"bytes,2,opt,name=key,proto3" json:"key" xorm:"VARCHAR(64) NOT NULL DEFAULT ” comment('配置项名称')"`
	// @inject_tag: json:"desc" xorm:"TEXT comment('配置项描述')"
	Desc string `protobuf:"bytes,3,opt,name=desc,proto3" json:"desc" xorm:"TEXT comment('配置项描述')"`
	// @inject_tag: json:"value" xorm:"TEXT comment('配置项值')"
	Value string `protobuf:"bytes,4,opt,name=value,proto3" json:"value" xorm:"TEXT comment('配置项值')"`
	// @inject_tag: json:"status" xorm:"INT notnull default(0) comment('配置项状态，1：有效；2：无效')"
	Status int32 `protobuf:"varint,5,opt,name=status,proto3" json:"status" xorm:"INT notnull default(0) comment('配置项状态，1：有效；2：无效')"`
	// @inject_tag: json:"create_time" xorm:"TIMESTAMP notnull created comment('记录创建时间')"
	CreateTime string `protobuf:"bytes,6,opt,name=create_time,proto3" json:"create_time" xorm:"TIMESTAMP notnull created comment('记录创建时间')"`
	// @inject_tag: json:"update_time" xorm:"TIMESTAMP notnull updated comment('记录变更时间')"
	UpdateTime string `protobuf:"bytes,7,opt,name=update_time,proto3" json:"update_time" xorm:"TIMESTAMP notnull updated comment('记录变更时间')"`
}

func (x *Config) Reset() {
	*x = Config{}
	if protoimpl.UnsafeEnabled {
		mi := &file_module_config_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Config) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Config) ProtoMessage() {}

func (x *Config) ProtoReflect() protoreflect.Message {
	mi := &file_module_config_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Config.ProtoReflect.Descriptor instead.
func (*Config) Descriptor() ([]byte, []int) {
	return file_module_config_proto_rawDescGZIP(), []int{0}
}

func (x *Config) GetId() int32 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *Config) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Config) GetDesc() string {
	if x != nil {
		return x.Desc
	}
	return ""
}

func (x *Config) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

func (x *Config) GetStatus() int32 {
	if x != nil {
		return x.Status
	}
	return 0
}

func (x *Config) GetCreateTime() string {
	if x != nil {
		return x.CreateTime
	}
	return ""
}

func (x *Config) GetUpdateTime() string {
	if x != nil {
		return x.UpdateTime
	}
	return ""
}

var File_module_config_proto protoreflect.FileDescriptor

var file_module_config_proto_rawDesc = []byte{
	0x0a, 0x13, 0x6d, 0x6f, 0x64, 0x75, 0x6c, 0x65, 0x2f, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x6d, 0x6f, 0x64, 0x75, 0x6c, 0x65, 0x22, 0xb0, 0x01,
	0x0a, 0x06, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x05, 0x52, 0x02, 0x69, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x65,
	0x73, 0x63, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x64, 0x65, 0x73, 0x63, 0x12, 0x14,
	0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x05, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x20, 0x0a, 0x0b,
	0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0b, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x12, 0x20,
	0x0a, 0x0b, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x07, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0b, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65,
	0x42, 0x28, 0x5a, 0x26, 0x70, 0x63, 0x6c, 0x2d, 0x73, 0x64, 0x6b, 0x2d, 0x67, 0x6f, 0x2d, 0x75,
	0x72, 0x63, 0x68, 0x69, 0x6e, 0x2f, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2f, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x2f, 0x6d, 0x6f, 0x64, 0x75, 0x6c, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_module_config_proto_rawDescOnce sync.Once
	file_module_config_proto_rawDescData = file_module_config_proto_rawDesc
)

func file_module_config_proto_rawDescGZIP() []byte {
	file_module_config_proto_rawDescOnce.Do(func() {
		file_module_config_proto_rawDescData = protoimpl.X.CompressGZIP(file_module_config_proto_rawDescData)
	})
	return file_module_config_proto_rawDescData
}

var file_module_config_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_module_config_proto_goTypes = []any{
	(*Config)(nil), // 0: module.Config
}
var file_module_config_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_module_config_proto_init() }
func file_module_config_proto_init() {
	if File_module_config_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_module_config_proto_msgTypes[0].Exporter = func(v any, i int) any {
			switch v := v.(*Config); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_module_config_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_module_config_proto_goTypes,
		DependencyIndexes: file_module_config_proto_depIdxs,
		MessageInfos:      file_module_config_proto_msgTypes,
	}.Build()
	File_module_config_proto = out.File
	file_module_config_proto_rawDesc = nil
	file_module_config_proto_goTypes = nil
	file_module_config_proto_depIdxs = nil
}