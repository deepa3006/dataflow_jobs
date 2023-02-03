import apache_beam as beam
import argparse

def find_apple(line,term):
    if term in line:
        return line
    
PROJECT='utility-cumulus-372111'
BUCKET='dataflow-bucket-deepa/data'
REGION='us-central1'

def run():
    argv=[
        '--project={0}.format(PROJECT)',
        '--region={0}.format(REGION)',
        '--job_name=find-fruit-job',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/staging/'.format(BUCKET),
        '--runner=DataflowRunner'
    ]

    p= p= beam.pipeline(argv=argv)
    
    input_prefix='gs://{0}/data/fruits.csv'.format(BUCKET)
    output_prefix='gs://{0}/output/fruits/apples'.format(BUCKET)

    searchterm='Apples'

    (p
       |'ReadFile' >> beam.io.ReadFromText(input_prefix)
       | 'GetAppleFruit' >> beam.FlatMap(lambda line: find_apple(line,searchterm))
       | 'WriteToFile' >> beam.io.WriteToText(output_prefix)
    
    )

    
    p.run()

    if __name__== '__main__':
        run()