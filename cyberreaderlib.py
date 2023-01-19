#!/usr/bin/python3
from os.path import exists
import sys
import struct
import google.protobuf as protobuf
import google.protobuf.descriptor_pb2 as descriptor_pb2
import apollopy.proto.record_pb2 as record_pb2
import apollopy.proto.proto_desc_pb2 as proto_desc_pb2
import json
from google.protobuf.json_format import MessageToJson


class Section:
    def __init__(self) -> None:
        self.type = -1
        self.size = 0

class RecordFileBase:
    def __init__(self) -> None:
        self.path = None
        self.header = record_pb2.Header()
        self.index = record_pb2.Index()
        self.f = None

    def Open(self) -> bool:
        raise NotImplementedError()

    def Close(self) -> None:
        raise NotImplementedError()

    def CurrentPosition(self) -> int:
        return self.f.tell()

    def SetPosition(self, pos) -> bool:
        self.f.seek(pos, 0)
        if self.CurrentPosition() != pos:
            return False

        return True

class RecordFileReader(RecordFileBase):
    SIZEOF_SECTION = 16
    HEADER_LENGTH = 2048

    def __init__(self) -> None:
        self.end_of_file = False

    def Open(self, path) -> bool:
        if not exists(path):
            return False

        self.f = open(path, mode="rb")
        self.end_of_file = False
        if not self.ReadHeader():
            return False

        return True

    def Close(self) -> None:
        self.f.close()

    def Reset(self) -> bool:
        if not self.SetPosition(RecordFileReader.SIZEOF_SECTION + RecordFileReader.HEADER_LENGTH):
            return False

        self.end_of_file = False
        return True

    def ReadHeader(self) -> bool:
        section = Section()
        if not self.ReadSection(section):
            return False

        if section.type != record_pb2.SECTION_HEADER:
            return False

        header = record_pb2.Header()
        if not self.ReadSectionT(section.size, header):
            return False
        
        if not self.SetPosition(RecordFileReader.SIZEOF_SECTION + RecordFileReader.HEADER_LENGTH):
            return False
        
        self.header = header
        return True

    def ReadIndex(self) -> bool:
        if not self.header.is_complete:
            return False
        
        if not self.SetPosition(self.header.index_position):
            return False
        
        section = Section()
        if not self.ReadSection(section):
            return False

        if section.type != record_pb2.SECTION_INDEX:
            return False

        index = record_pb2.Index()
        if not self.ReadSectionT(section.size, index):
            return False

        self.Reset()
        self.index = index
        return True

    def ReadSection(self, section) -> Section:
        try:
            section_bytes = self.f.read(RecordFileReader.SIZEOF_SECTION)
            if len(section_bytes) == 0:
                self.end_of_file = True
                return False
            elif len(section_bytes) != RecordFileReader.SIZEOF_SECTION:
                return False
        except:
            return False

        section.type = struct.unpack_from("@i", section_bytes, 0)[0]
        section.size = struct.unpack_from("=q", section_bytes, 8)[0]
        return True

    def ReadSectionT(self, size, message) -> bool:
        data = self.f.read(size)
        message.ParseFromString(data)
        return True

    def SkipSection(self, size) -> bool:
        pos = self.CurrentPosition()
        if size > sys.maxsize - pos:
            return False
        
        if not self.SetPosition(pos + size):
            return False

        return True

class RecordMessage:
    def __init__(self) -> None:
        self.channel_name = None
        self.content = None
        self.time = 0

class RecordBase:
    def __init__(self) -> None:
        self.file = None
        self.header = record_pb2.Header()
        self.is_opened = False

    def GetMessageNumber(self, channel_name) -> int:
        raise NotImplementedError()
    
    def GetMessageType(self, channel_name) -> str:
        raise NotImplementedError()

    def GetProtoDesc(self, channel_name) -> str:
        raise NotImplementedError()

    def GetChannelList(self) -> list:
        raise NotImplementedError()

class RecordReader(RecordBase):
    def __init__(self, file) -> None:
        self.is_valid = False
        self.reach_end = False
        self.chunk = None
        self.index = record_pb2.Index()
        self.message_index = 0
        self.channel_info = { }
        self.file_reader = RecordFileReader()

        if not self.file_reader.Open(file):
            return

        self.chunk = record_pb2.ChunkBody()
        self.is_valid = True
        self.header = self.file_reader.header
        if self.file_reader.ReadIndex():
            self.index = self.file_reader.index
            for single_idx in self.index.indexes:
                if single_idx.type != record_pb2.SECTION_CHANNEL:
                    continue

                if not single_idx.HasField("channel_cache"):
                    continue

                channel_cache = single_idx.channel_cache
                self.channel_info[channel_cache.name] = channel_cache

        self.file_reader.Reset()

    def Reset(self) -> None:
        self.file_reader.Reset()
        self.reach_end = False
        self.message_index = 0
        self.chunk = record_pb2.ChunkBody()

    def GetChannelList(self) -> list:
        return list(self.channel_info.keys())

    def ReadMessage(self, message, begin_time = 0, end_time = sys.maxsize) -> bool:
        if not self.is_valid:
            return False
        
        if (begin_time > self.header.end_time) or (end_time < self.header.begin_time):
            return False

        while self.message_index < len(self.chunk.messages):
            next_message = self.chunk.messages[self.message_index]
            time = next_message.time
            if time > end_time:
                return False

            self.message_index += 1
            if time < begin_time:
                continue

            message.channel_name = next_message.channel_name
            message.content = next_message.content
            message.time = time
            return True

        if self.ReadNextChunk(begin_time, end_time):
            self.message_index = 0
            return self.ReadMessage(message, begin_time, end_time)

        return False

    def ReadNextChunk(self, begin_time, end_time) -> bool:
        skip_next_chunk_body = False
        while not self.reach_end:
            section = Section()
            if not self.file_reader.ReadSection(section):
                return False

            if section.type == record_pb2.SECTION_INDEX:
                self.file_reader.SkipSection(section.size)
                self.reach_end = True
            elif section.type == record_pb2.SECTION_CHANNEL:
                channel = record_pb2.Channel()
                if not self.file_reader.ReadSectionT(section.size, channel):
                    return False
            elif section.type == record_pb2.SECTION_CHUNK_HEADER:
                header = record_pb2.ChunkHeader()
                if not self.file_reader.ReadSectionT(section.size, header):
                    return False
                if header.end_time < begin_time:
                    skip_next_chunk_body = True
                if header.begin_time > end_time:
                    return False
            elif section.type == record_pb2.SECTION_CHUNK_BODY:
                if skip_next_chunk_body:
                    self.file_reader.SkipSection(section.size)
                    skip_next_chunk_body = False
                else:
                    self.chunk = record_pb2.ChunkBody()
                    if not self.file_reader.ReadSectionT(section.size, self.chunk):
                        return False
                    return True
            else:
                return False
        return False

    def GetMessageNumber(self, channel_name) -> int:
        search = self.channel_info[channel_name]
        if search is None:
            return 0

        return search.message_number

    def GetMessageType(self, channel_name) -> str:
        search = self.channel_info[channel_name]
        if search is None:
            return None

        return search.message_type

    def GetProtoDesc(self, channel_name) -> str:
        search = self.channel_info[channel_name]
        if search is None:
            return None

        return search.proto_desc

class ProtobufFactory:
    def __init__(self) -> None:
        self.pool = protobuf.descriptor_pool.DescriptorPool()
        self.factory = protobuf.message_factory.MessageFactory(self.pool)

    def RegisterMessage(self, proto_desc_str) -> bool:
        proto_desc = proto_desc_pb2.ProtoDesc()
        proto_desc.ParseFromString(proto_desc_str)
        return self.RegisterMessageProtoDesc(proto_desc)

    def RegisterMessageProtoDesc(self, proto_desc) -> bool:
        for dep in proto_desc.dependencies:
            self.RegisterMessageProtoDesc(dep)

        file_desc_proto = descriptor_pb2.FileDescriptorProto()
        file_desc_proto.ParseFromString(proto_desc.desc)
        return self.RegisterMessageFileDescriptorProto(file_desc_proto)

    def RegisterMessageFileDescriptorProto(self, file_desc_proto) -> bool:
        self.pool.Add(file_desc_proto)
        return True

    def GenerateMessageByType(self, type) -> protobuf.message.Message:
        try:
            descriptor = self.pool.FindMessageTypeByName(type)
            if descriptor is None:
                return None

            prototype = self.factory.GetPrototype(descriptor)
            if prototype is None:
                return None

            return prototype()
        except:
            return None

if __name__ == "__main__":
    filename = sys.argv[1]
    unqiue_channel = []
    pbfactory = ProtobufFactory()
    reader = RecordReader(filename)
    for channel in reader.GetChannelList():
        desc = reader.GetProtoDesc(channel)
        pbfactory.RegisterMessage(desc)
        unqiue_channel.append(channel)
        
    message = RecordMessage()
    count = 0
    while reader.ReadMessage(message):
        message_type = reader.GetMessageType(message.channel_name)
        msg = pbfactory.GenerateMessageByType(message_type)
        msg.ParseFromString(message.content)
        print(message.channel_name)
        if(message.channel_name == "/tf" or
            message.channel_name == "/apollo/sensor/gnss/raw_data" or
            message.channel_name == "/apollo/sensor/gnss/corrected_imu" or
            message.channel_name == "/apollo/localization/pose"):
            #print("msg[%d]-> channel name: %s; message type: %s; message time: %d, content: %s" % (count, message.channel_name, message_type, message.time, msg))
            jdata = MessageToJson(msg)
            print(jdata)
    print("Message Count %d" % count)
