import apache_beam as beam

from apache_beam.options.pipeline_options import GoogleCloudOptions
import google.auth
import json
from apache_beam.options import pipeline_options

options = pipeline_options.PipelineOptions()

# Set the pipeline mode to stream the data from Pub/Sub.
options.view_as(pipeline_options.StandardOptions).streaming = True

schema = 'event:JSON' 

p = beam.Pipeline( options=options)
events = p | "read" >> beam.io.ReadFromPubSub(topic="projects/utility-cumulus-372111/topics/random-events")                                                                    

windowed_events = (events
| 'to json' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
| 'represent as TableRow json (column_name: value)' >> beam.Map(lambda x: "{\"event\": %s}" %json.dumps(json.dumps(x)))
| 'back to json' >> beam.Map(lambda x: json.loads(x))
| 'Write to BQ' >> beam.io.WriteToBigQuery(                  
  'utility-cumulus-372111.dataflowsink.jevents',
   schema=schema,
   ignore_unknown_columns=True,
   create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,    
   write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
)

p.run()
