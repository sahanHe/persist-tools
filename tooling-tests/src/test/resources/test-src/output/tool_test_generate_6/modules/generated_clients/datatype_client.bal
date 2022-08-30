import ballerina/sql;
import ballerinax/mysql;
import ballerina/time;
import ballerina/persist;
import foo/perist_generate_6 as entities;

public client class DataTypeClient {

    private final string entityName = "DataType";
    private final sql:ParameterizedQuery tableName = `DataTypes`;

    private final map<persist:FieldMetadata> fieldMetadata = {
        a: {columnName: "a", 'type: int, autoGenerated: true},
        b1: {columnName: "b1", 'type: string},
        c1: {columnName: "c1", 'type: int},
        d1: {columnName: "d1", 'type: boolean},
        e1: {columnName: "e1", 'type: float},
        f1: {columnName: "f1", 'type: decimal},
        j1: {columnName: "j1", 'type: time:Utc},
        k1: {columnName: "k1", 'type: time:Civil},
        l1: {columnName: "l1", 'type: time:Date},
        m1: {columnName: "m1", 'type: time:TimeOfDay},
        v1: {columnName: "v1", 'type: anydata}
    };

    private string[] keyFields = ["a"];

    private persist:SQLClient persistClient;

    public function init() returns error? {
        mysql:Client dbClient = check new (host = host, user = user, password = password, database = database, port = port);
        self.persistClient = check new (self.entityName, self.tableName, self.fieldMetadata, self.keyFields, dbClient);
    }

    remote function create(entities:DataType value) returns int|error? {
        sql:ExecutionResult result = check self.persistClient.runInsertQuery(value);
        return <int>result.lastInsertId;
    }

    remote function readByKey(int key) returns entities:DataType|error {
        return (check self.persistClient.runReadByKeyQuery(entities:DataType, key)).cloneWithType(entities:DataType);
    }

    remote function read(map<anydata>? filter = ()) returns stream<entities:DataType, error?>|error {
        stream<anydata, error?> result = check self.persistClient.runReadQuery(entities:DataType, filter);
        return new stream<entities:DataType, error?>(new DataTypeStream(result));
    }

    remote function update(record {} 'object, map<anydata> filter) returns error? {
        _ = check self.persistClient.runUpdateQuery('object, filter);
    }

    remote function delete(map<anydata> filter) returns error? {
        _ = check self.persistClient.runDeleteQuery(filter);
    }

    function close() returns error? {
        return self.persistClient.close();
    }
}

public class DataTypeStream {

    private stream<anydata, error?> anydataStream;

    public isolated function init(stream<anydata, error?> anydataStream) {
        self.anydataStream = anydataStream;
    }

    public isolated function next() returns record {|entities:DataType value;|}|error? {
        var streamValue = self.anydataStream.next();
        if streamValue is () {
            return streamValue;
        } else if (streamValue is error) {
            return streamValue;
        } else {
            record {|entities:DataType value;|} nextRecord = {value: check streamValue.value.cloneWithType(entities:DataType)};
            return nextRecord;
        }
    }

    public isolated function close() returns error? {
        return self.anydataStream.close();
    }
}
