# Data Transformations

When we import the entire set of historic data, there are a number of old and differing record structures to what we see when new data is sent across Kafka.

This component's job is to ingest the historic UC data and make it look like data that came from Kafka. This allows us to have a full historic record linked together for every document.

Due to the changes in data structures between the two sources we have to make changes to the historic data in order to achieve this historic record per document.

## Why not change Kafka data?

The Kafka stream is considered golden and will always be using the latest data structure. Therefore the principal the system follows is to take the Kafka data as-is and instead change historic data to match it rather than the other way around.

The historic data ingestion is a one time process and will not need to be repeated after the system is up and running.

## Transformations performed

### Row level data transforms

The data transformations performed are stored in detail in the following Jira epic: https://projects.ucd.gpn.gov.uk/browse/DW-3769.

Below is a table listing the transformations that are performed on the historic data on a row by row basis.
For details see the stories in the above Epic.

In general, as a rough description but not always, the pre-transform example matches what we see in the UC Data Dumps (and thus what goes to Crown currently directly from them), and the Post-transform examples match what we would see in the Kafka feed.

| Data Transformation Type | Pre-Transform Example | Post-Transform Example |
| ------------------- | --------------------- | ---------------------- |
| Strip dates with `$date` fields | `{ "_lastModifiedDateTime": {"$date": "2019-01-01T01:01:01.000Z" }}` | `{ "_lastModifiedDateTime": "2019-01-01T01:01:01.000+0000" }` |
| Strip ids with `$oid` fields | `{ "_id": {"$oid": "guid1" }}` | `{ "_id": "guid1" }` |
| Hierarchy for `_lastModifiedDateTime` | `{ "_lastModifiedDateTime": "", "createdDateTime": "date1" }` | `{ "_lastModifiedDateTime": "date1", "createdDateTime": "date1" }` |
| Re-structure `_removed` records | `{ "_id": {"$oid": "guid1" }}, "_removed": "[entire record]"` | `{ [entire record] }` |
| Re-structure `_archived` records | `{ "_id": {"$oid": "guid1" }}, "_archived": "[entire record]"` | `{ [entire record] }` |
| Flatten date objects within `_ids`| `{ "_id": { "idField": "idValue", "createdDateTime": { "$date": "2019-01-01T01:01:01.000Z" } } }` | `{ "_id": { "idField": "idValue", "createdDateTime": "2019-01-01T01:01:01.000+0000" } }` |

### Database and Collections Transforms

Below is a table listing the transformations that are performed on the historic data on a Collection level. 

As UC have some very large data sets, they have both fixed Archives and also split some tarball deliverables into files as if they are pseudo-collections numbered 0001-1000. 

| Collection Transformation Type | Transform |
| ------------------- | ---------------------- |
| Coalesce split collections `aa.bb-1` thru `aa.bb-N` | Put all data for source files `dump.aa.bb-1.gz`, `dump.aa.bb-2.gz` into table `aa:bb` |
| Coalesce `agentToDoArchive` into `agentToDo` | put all data for source files `dump.agentToDoArchive.xx-1,2,3.gz` into table `agentToDo:xx |


## Transformation details and reasoning

Below is a detailed explanation of the change made for each transform. Each transform is independant of other transforms and multiple transforms might happen to the same record.

### Strip dates with `$date` fields

#### Transform details

The following keys are checked to see if they are strings or objects: `_lastModifiedDateTime`, `createdDateTime`, `_removedDateTime` and `_archivedDateTime`. If they are present and strings no transformation is performed. If they are objects with a `$date` sub key, the value of the sub key is used as the parent key field value.
Additionally the string has its final 'Z' replaced with '+0000' to match the kafka style.

#### Transform reasoning

These date formats are added by Mongo's export process when the data is just a string. On Kafka, the `$date` field is not present and these date values appear as the post transform example. For the storage of records, a consistent date time structure is needed and this is used to ensure which version of the record is the latest version.

### Strip ids with `$oid` fields

#### Transform details

The following key is checked to see if it is a string value or an object value: `_id`. If it is a string no transformation is performed. If it is an object, then it is checked to see if it has a `$oid` sub key. If it doesn't, then no transformation is performed.

If it does, then the value of the `$oid` sub key is taken and used as the parent key field value.

#### Transform reasoning

When Mongo exports records, if it has no primary key as the id, then it generates an id and calls the key `$oid`. Therefore, this value appears in the data as the `_id` field. However on Kafka, this is stripped out and the `$oid` key is never seen, only it's value. Therefore we replicate this process.

### Heirarchy for `_lastModifiedDateTime`

#### Transform details

When a record is receieved, the `_lastModifiedDateTime` field is checked. If it has an invalid value (null or empty), then the `createdDateTime` field is checked instead. If it is valid, then its value is used for `_lastModifiedDateTime`. It neither are present we use a fall back date of "1980-01-01T00:00:00.000Z".

#### Transform reasoning

To store all versions of records, there needs to be a timestamp assigned to every version of a record. To do this, we use the `_lastModifiedDateTime`. Some older records do not have this field in them. If this was not transformed, potentially these versions of the records would be stored out of order, creating data issues when the latest version of every record was desired. This transform ensures the integrity of the record history is maintained.

### Re-structure `_removed` records

#### Transform details

When a record is processed, it is checked to see if it has a `_removed` key. If that's the case, the following steps are performed to transform the record:

* If the root level has a `_lastModifiedDateTime` field, move this to be at the root of the `_removed` node
* If the root level has a `_removedDateTime` field, move this to be at the root of the `_removed` node
* If the root level has a `timestamp` field, move this to be at the root of the `_removed` node
* Remove the `_removed` node and place all it's nodes up one to the root

This means that an incoming record with the following structure:

```
{
  "_id": {
    "$oid": "someoid"
  },
  "_lastModifiedDateTime": {
    "$date": "2020-02-26T10:04:39.624Z"
  },
  "_removedDateTime": {
    "$date": "2020-02-26T10:04:39.623Z"
  },
  "timestamp": 1582711479624,
  "_removed": {
    "_entityVersion": 0,
    "_id": {
      "toDoId": "guid1"
    },
    "_version": 1,
    ...
  }
}
```

Would be transformed to the following structure:

```
{
  "_entityVersion": 0,
  "_id": {
    "toDoId": "guid1"
  },
  "_version": 1,
  "_lastModifiedDateTime": {
    "$date": "2020-02-26T10:04:39.624Z"
  },
  "_removedDateTime": {
    "$date": "2020-02-26T10:04:39.623Z"
  },
  "timestamp": 1582711479624,
  ...
}
```

As explained above, there are other transforms that would edit the dates above, these would still happen but that is independant of this specific transform step.

#### Transform reasoning

When a record is soft deleted, the Kafka stream receives a MONGO_DELETE record, which is the delete notification containing the record details as is before the delete has been actioned. However in the historic data, the record looks like it does _after_ the delete has happened. This means the structure is very different between the two records. Due to the UC soft delete process where the record is moved to a `_removed` key, it also means there is no primary key any more on the record. When Mongo exports this record it therefore assigns a new id to it as an `$oid` key.

If we did not transform these records, then the historic records would appear as a completely different record in the storage because of this generated id and you would have two records stored:

1. The original record with the right id and all versions up to prior to the deletion (so with no `_removedDateTime` populated)
2. A new record with an entirely different id and not linked to the previous versions that would hold the `_removedDateTime` value

### Re-structure `_archived` records

#### Transform details

When a record is processed, it is checked to see if it has a `_archived` key. If that's the case, the following steps are performed to transform the record:

* If the root level has a `_lastModifiedDateTime` field, move this to be at the root of the `_archived` node
* If the root level has a `_archivedDateTime` field, move this to be at the root of the `_archived` node
* If the root level has a `timestamp` field, move this to be at the root of the `_archived` node
* Remove the `_archived` node and place all it's nodes up one to the root

This means that an incoming record with the following structure:

```
{
  "_id": {
    "$oid": "someoid"
  },
  "_lastModifiedDateTime": {
    "$date": "2020-02-26T10:04:39.624Z"
  },
  "_archivedDateTime": {
    "$date": "2020-02-26T10:04:39.623Z"
  },
  "timestamp": 1582711479624,
  "_archived": {
    "_entityVersion": 0,
    "_id": {
      "toDoId": "guid1"
    },
    "_version": 1,
    ...
  }
}
```

Would be transformed to the following structure:

```
{
  "_entityVersion": 0,
  "_id": {
    "toDoId": "guid1"
  },
  "_version": 1,
  "_lastModifiedDateTime": {
    "$date": "2020-02-26T10:04:39.624Z"
  },
  "_archivedDateTime": {
    "$date": "2020-02-26T10:04:39.623Z"
  },
  "timestamp": 1582711479624,
  ...
}
```

As explained above, there are other transforms that would edit the dates above, these would still happen but that is independant of this specific transform step.

#### Transform reasoning

When a record is archived, the Kafka stream receives a MONGO_DELETE record, which is the delete notification containing the record details as is before the delete has been actioned. However in the historic data, the record looks like it does _after_ the delete has happened. This means the structure is very different between the two records. Due to the UC archive process where the record is moved to a `_archived` key, it also means there is no primary key any more on the record. When Mongo exports this record it therefore assigns a new id to it as an `$oid` key.

If we did not transform these records, then the historic records would appear as a completely different record in the storage because of this generated id and you would have two records stored:

1. The original record with the right id and all versions up to prior to the deletion (so with no `_archivedDateTime` populated)
2. A new record with an entirely different id and not linked to the previous versions that would hold the `_archivedDateTime` value

### Flatten date objects within `_ids`
#### Transform details
In some collections the `_id` field is a compound object one of whose fields, which could be `_lastModifiedDateTime`, 
`createdDateTime`, `_removedDateTime` and `_archivedDateTime` is a mongo date field. 
These date fields are flattened, i.e. the `createdDateTimeValue is set to the value of the `$date` sub-field. 
Additionally the date string itself is converted from "2020-02-26T10:04:39.624Z" format (i.e. with a 'Z' at the end) to
"2020-02-26T10:04:39.624+0000" format to match what comes across on kafka.
For example:
```javascript
{
    "_id": {
        "idField": "idValue",
        "createdDateTime": {
            "$date": "2020-02-26T10:04:39.624Z"    
        }       
    }
}
``` 
becomes
```javascript
{
    "_id": {
        "idField": "idValue",
        "createdDateTime": "2020-02-26T10:04:39.624+0000"
    }
}
``` 

#### Transform reasoning
If these records come over kafka following a modification they will be in the transformed format, and so to ensure that
we add a new version to the correct record the ids must match and so we make id of the record imported from the dump
look like it's kafka equivalent before persisting it to hbase.

### Coalesce split collections
#### Transform details
UC split some collections on export into 33 separate sets of files that look like they come from different collections.
For example calculator.calculationParts is exported into sets of files called

 calculator.calculationParts-one. ...
 calculator.calculationParts-two. ...
 ...
 calculator.calculationParts-thirtytwo. ...
 calculator.calculationParts-archive. ...

HDI needs to ensure that these files are imported into calculator.calculationParts and so when the table name is derived
from the filename the trailing 'one', 'two' etc is stripped off meaning that the derived table name for all these files
is calculator:calculationParts.

### Coalesce agentToDoArchive into agentToDo
#### Transform details
In the UC system record from agentToDo are moved into agentToDoArchive by a batch process and these inserts into 
agentToDoArchive are not published to the kafka stream. In crown these two collections are merged back together.
We are going to do this merge on import so that we have one table agentToDo that contains all records from agentToDo
and agentToDoArchive.

#### Transform reasoning
