#pragma once

#include "common/serializer/serializer.h"
#include "common/types/types.h"
#include "gopt/g_constants.h"
#include <yaml-cpp/yaml.h>

namespace kuzu {
class LogicalType;
class kuzu::common::Serializer;

class VarcharExtraInfo : public kuzu::common::ExtraTypeInfo {
private:
    uint64_t maxLength;

protected:
    void serializeInternal(kuzu::common::Serializer& serializer) const override {}

public:
    explicit VarcharExtraInfo(uint64_t maxLength) : maxLength{maxLength} {}
    ~VarcharExtraInfo() override = default;
    bool containsAny() const override { return false; }

    bool operator==(const ExtraTypeInfo& other) const override {
        return maxLength == other.constPtrCast<VarcharExtraInfo>()->maxLength;
    }

    std::unique_ptr<ExtraTypeInfo> copy() const override {
        return std::make_unique<VarcharExtraInfo>(maxLength);
    }
};

class GTypeUtils {
public:
    static inline kuzu::common::LogicalType createLogicalType(YAML::Node& node) {
        // Implementation of createLogicalType
        // This function should parse the YAML node and create a LogicalType object.
        // The actual implementation is not provided in the original code snippet.
        auto primitiveType = node["primitive_type"];
        if (primitiveType) {
            auto typeStr = primitiveType.as<std::string>();
            if (typeStr == "DT_SIGNED_INT64") {
                return kuzu::common::LogicalType(kuzu::common::LogicalTypeID::INT64);
            } else if (typeStr == "DT_UNSIGNED_INT64") {
                return kuzu::common::LogicalType(kuzu::common::LogicalTypeID::UINT64);
            } else if (typeStr == "DT_SIGNED_INT32") {
                return kuzu::common::LogicalType(kuzu::common::LogicalTypeID::INT32);
            } else if (typeStr == "DT_UNSIGNED_INT32") {
                return kuzu::common::LogicalType(kuzu::common::LogicalTypeID::UINT32);
            } else if (typeStr == "DT_FLOAT") {
                return kuzu::common::LogicalType(kuzu::common::LogicalTypeID::FLOAT);
            } else if (typeStr == "DT_DOUBLE") {
                return kuzu::common::LogicalType(kuzu::common::LogicalTypeID::DOUBLE);
            } else if (typeStr == "DT_BOOL") {
                return kuzu::common::LogicalType(kuzu::common::LogicalTypeID::BOOL);
            }
        }
        auto stringType = node["string"];
        if (stringType) {
            // denote varchar
            return kuzu::common::LogicalType(kuzu::common::LogicalTypeID::STRING);
            // auto varchar = stringType["var_char"];
            // if (varchar) {
            //     uint64_t maxLength = Constants::VARCHAR_MAX_LENGTH;
            //     if (varchar["max_length"]) {
            //         maxLength = varchar["max_length"].as<uint64_t>();
            //     }
            //     auto varcharType =
            //     kuzu::common::LogicalType(kuzu::common::LogicalTypeID::STRING);
            //     varcharType.setExtraTypeInfo(std::make_unique<VarcharExtraInfo>(maxLength));
            //     return varcharType;
            // }
        }
        if (node["temporal"]) {
            return kuzu::common::LogicalType(kuzu::common::LogicalTypeID::INT64);
        }
        return kuzu::common::LogicalType(kuzu::common::LogicalTypeID::ANY);
    }
};
} // namespace kuzu