// AUTO-GENERATED FILE. DO NOT MODIFY.

// This file is an auto-generated file by Ballerina persistence layer for tool_test_db_push_25.
// It should not be modified by hand.

import ballerina/time;

public type MedicalNeed record {|
    readonly int needId;
    int itemId;
    int beneficiaryId;
    time:Civil period;
    string urgency;
    int quantity;
|};

public type MedicalNeedInsert MedicalNeed;

public type MedicalNeedUpdate record {|
    int itemId?;
    int beneficiaryId?;
    time:Civil period?;
    string urgency?;
    int quantity?;
|};

public type MedicalItem record {|
    readonly int itemId;
    string name;
    string 'type;
    string unit;
|};

public type MedicalItemInsert MedicalItem;

public type MedicalItemUpdate record {|
    string name?;
    string 'type?;
    string unit?;
|};

