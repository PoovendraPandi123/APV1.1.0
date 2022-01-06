def get_field_extraction_delimiter_operators(transformation_operators_list):
    try:
        if len(transformation_operators_list) > 0:
            for transformation_operators in transformation_operators_list:
                # print(transformation_operators)
                if transformation_operators["operator_key"] == "delimiter_operators":
                    return transformation_operators["operator_value"]
        else:
            print("Length of Transformation Operators List is equals to Zero!!!")
            return ''
    except Exception:
        return ""

transformation_operators_list = [
    {
        "id": 1,
        "tenants_id": 1,
        "groups_id": 1,
        "entities_id": 1,
        "operator_key": "field_extraction_operators",
        "operator_value": [
            {
                "key": "separator",
                "value": "delimiter"
            },
            {
                "key": "custom",
                "value": "custom"
            }
        ],
        "is_active": True,
        "created_by": 1,
        "modified_by": 1
    },
    {
        "id": 2,
        "tenants_id": 1,
        "groups_id": 1,
        "entities_id": 1,
        "operator_key": "delimiter_operators",
        "operator_value": [
            {
                "key": "tab",
                "value": "\t"
            },
            {
                "key": "space",
                "value": " "
            },
            {
                "key": "comma",
                "value": ","
            },
            {
                "key": "hiffen",
                "value": "-"
            },
            {
                "key": "forward_slash",
                "value": "/"
            },
            {
                "key": "backward_slash",
                "value": "\\"
            },
            {
                "key": "colon",
                "value": ":"
            },
            {
                "key": "semicolon",
                "value": ";"
            },
            {
                "key": "astrics",
                "value": "*"
            }
        ],
        "is_active": True,
        "created_by": 1,
        "modified_by": 1
    },
    {
        "id": 3,
        "tenants_id": 1,
        "groups_id": 1,
        "entities_id": 1,
        "operator_key": "custom_operators",
        "operator_value": [
            {
                "key": "small_char",
                "value": "[a-z]"
            },
            {
                "key": "big_char",
                "value": "[A-Z]"
            },
            {
                "key": "natural_numbers",
                "value": "[1-9]"
            },
            {
                "key": "whole_numbers",
                "value": "[0-9]"
            }
        ],
        "is_active": True,
        "created_by": 1,
        "modified_by": 1
    },
    {
        "id": 4,
        "tenants_id": 1,
        "groups_id": 1,
        "entities_id": 1,
        "operator_key": "transaction_operators",
        "operator_value": [
            {
                "key": "starts_with",
                "value": "{reference}%"
            },
            {
                "key": "ends_with",
                "value": "%{reference}"
            },
            {
                "key": "inbetween",
                "value": "%{reference}%"
            }
        ],
        "is_active": True,
        "created_by": 1,
        "modified_by": 1
    }
]

print(get_field_extraction_delimiter_operators(transformation_operators_list))