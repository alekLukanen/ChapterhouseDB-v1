## Subscription Merge Filters

These are functions which allow you to filter out incoming events based on the event's
row data and the existing row data in the parquet file. For example, if an event arrives late
you can use one of the filters to prevent that event from being applied to the main-line.
This ensures that the data in the parquet file is always correct.

### Implementation

These filters will be added at the subscription level and will be applied to all the
data for that subscription. When an event arrives it will be batched as usual, but when
it is merged into the main-line and a row already exists for the subscriptions partition
key then the filter will be applied and return a boolean value indicating if the new event
should be accepted or not after having already checked that the new row will be accepted 
by the existing comparison logic. For now there will only be pre-built filters, such
as the time based filtering for late arriving events. Later this can become more general
and allow for arbitrary user-defined functions. The basic idea is to allow the user
to define arbitrary functions that allow the user to filter out updates to rows
when the row already exists. These can be thought of as user-defined constraints.

To implement time based filtering of late arriving events the parquet file should contain a
new _produced_ts column that contains the events source created timestamp. Only propagate 
changes to the main-line files if the event _produced_ts is greater than the existing _produced_ts
in the main-line file. This will be a function that ensures that field is always
increasing in value for any particular unique row in the table.

