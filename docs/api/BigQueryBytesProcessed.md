# BigQueryBytesProcessed

## Properties
<div class="wy-table-responsive"><table border="1" class="docutils">
<thead>
<tr>
<th>Name</th>
<th>Type</th>
<th>Description</th>
<th>Notes</th>
</tr>
</thead>
<tbody>

Used to track number of BigQuery bytes processed in queries for a given project and date.




<tr>
    <td><strong>id</strong></td>
    <td><strong>int</strong></td>
    <td>Object ID</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>project</strong></td>
    <td><strong>str</strong></td>
    <td>GCP project ID</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>total</strong></td>
    <td><strong>int</strong></td>
    <td>Total bytes processed</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>date</strong></td>
    <td><strong>str</strong></td>
    <td>Date of queries</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>created</strong></td>
    <td><strong>datetime</strong></td>
    <td>Creation date</td>
    <td>[optional] [readonly] </td>
</tr>
<tr>
    <td><strong>modified</strong></td>
    <td><strong>datetime</strong></td>
    <td>Last modified date</td>
    <td>[optional] [readonly] </td>
</tr>


</tbody>
</table></div>

