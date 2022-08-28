import ballerina/sql;
import ballerinax/mysql;
import ballerina/persist;

client class MedicalItemClient {

    private final string entityName = "MedicalItem";
    private final sql:ParameterizedQuery tableName = `MedicalItems`;

    private final map<persist:FieldMetadata> fieldMetadata = {
        itemId: {columnName: "itemId", 'type: int},
        name: {columnName: "name", 'type: string},
        'type: {columnName: "type", 'type: string},
        unit: {columnName: "unit", 'type: string}
    };
    private string[] keyFields = ["itemId"];

    private persist:SQLClient persistClient;

    public function init() returns persist:Error? {
        mysql:Client dbClient = check new (host = HOST, user = USER, password = PASSWORD, database = DATABASE, port = PORT);
        self.persistClient = check new (self.entityName, self.tableName, self.fieldMetadata, self.keyFields, dbClient);
    }

    remote function create(MedicalItem value) returns int|persist:Error? {
        sql:ExecutionResult result = check self.persistClient.runInsertQuery(value);

        if result.lastInsertId is () {
            return value.itemId;
        }
        return <int>result.lastInsertId;
    }

    remote function readByKey(int key) returns MedicalItem|persist:Error {
        return (check self.persistClient.runReadByKeyQuery(MedicalItem, key)).cloneWithType(MedicalItem);
    }

    remote function read(map<anydata>? filter = ()) returns stream<MedicalItem, persist:Error?>|persist:Error {
        stream<anydata, error?> result = check self.persistClient.runReadQuery(filter);
        return new stream<MedicalItem, error?>(new MedicalItemStream(result));
    }

    remote function update(record {} 'object, map<anydata> filter) returns persist:Error? {
        _ = check self.persistClient.runUpdateQuery('object, filter);
    }

    remote function delete(map<anydata> filter) returns persist:Error? {
        _ = check self.persistClient.runDeleteQuery(filter);
    }

    function close() returns persist:Error? {
        return self.persistClient.close();
    }
}

public class MedicalItemStream {
    private stream<anydata, persist:Error?> anydataStream;

    public isolated function init(stream<anydata, persist:Error?> anydataStream) {
        self.anydataStream = anydataStream;
    }

    public isolated function next() returns record {|MedicalItem value;|}|persist:Error? {
        var streamValue = self.anydataStream.next();
        if streamValue is () {
            return streamValue;
        } else if (streamValue is error) {
            return streamValue;
        } else {
            record {|MedicalItem value;|} nextRecord = {value: check streamValue.value.cloneWithType(MedicalItem)};
            return nextRecord;
        }
    }

    public isolated function close() returns persist:Error? {
        return self.anydataStream.close();
    }
}
