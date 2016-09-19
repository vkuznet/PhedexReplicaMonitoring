# Weekly Reports 

## Week 10

- All configurable files in node app are kept in configuration file.
- Handling no data exceptions when visualizing data in charts.
- Implemented choice of chart x axis:
  - Node kind
  - Acquisiion era
  - Data tier
  - User group
- Implemented choice of chart y axis:
  - Node bytes 
  - Destination bytes
- Added ability to limit records visualized in chart. Remaining data is put in one group called "Other"
- Added possiblity to filter data by date (date range picker)
- Added possibility to choose elements from the list for filtering
  - Node kind
  - User group
- Added possibility to filter fields by regex
  - Acquisition era
  - Data tier
- Added two chart views for comparison
  - Both views have two charts: bar and pie.
  - Bar chart contains data of specified date range (Default: last week)
  - Pie chart contains data of one day (Default: last day)
- Added possibility to clear all fields by reset button
- Added possibility to download both chart areas data as JSON.
- Added possibility to select data bar in bar chart to redraw pie chart on selected day.
- Fixed issue that both charts (bar and pie) would use the same legend colors.
