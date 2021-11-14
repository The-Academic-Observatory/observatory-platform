# QueryResponse

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






<tr>
    <td><strong>version</strong></td>
    <td><strong>str</strong></td>
    <td>The API version</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>index</strong></td>
    <td><strong>str</strong></td>
    <td>The full name of the elasticsearch index that is searched</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>scroll_id</strong></td>
    <td><strong>str</strong></td>
    <td>The scroll id that can be used to get the next batch of results (only active for 1min)</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>returned_hits</strong></td>
    <td><strong>int</strong></td>
    <td>The number of returned hits (can be less than total_hits if a limit is set)</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>total_hits</strong></td>
    <td><strong>int</strong></td>
    <td>The number of total hits</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>schema</strong></td>
    <td><strong>bool, date, datetime, dict, float, int, list, str, none_type</strong></td>
    <td>The schema for an individual hit</td>
    <td>[optional] </td>
</tr>
<tr>
    <td><strong>results</strong></td>
    <td><strong>[bool, date, datetime, dict, float, int, list, str, none_type]</strong></td>
    <td>A list of the actual results (one dictionary per hit)</td>
    <td>[optional] </td>
</tr>


</tbody>
</table></div>

