import logging
import os
import csv

from neo4j import GraphDatabase
from retry import retry

HOSPITALS_CSV_PATH = os.getenv("HOSPITALS_CSV_PATH")
PAYERS_CSV_PATH = os.getenv("PAYERS_CSV_PATH")
PHYSICIANS_CSV_PATH = os.getenv("PHYSICIANS_CSV_PATH")
PATIENTS_CSV_PATH = os.getenv("PATIENTS_CSV_PATH")
VISITS_CSV_PATH = os.getenv("VISITS_CSV_PATH")
REVIEWS_CSV_PATH = os.getenv("REVIEWS_CSV_PATH")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

LOGGER = logging.getLogger(__name__)

NODES = ["Hospital", "Payer", "Physician", "Patient", "Visit", "Review"]


def _set_uniqueness_constraints(tx, node):
    query = f"""CREATE CONSTRAINT IF NOT EXISTS FOR (n:{node})
        REQUIRE n.id IS UNIQUE;"""
    _ = tx.run(query, {})


@retry(tries=100, delay=10)
def load_hospital_graph_from_csv() -> None:
    """Load structured hospital CSV data following
    a specific ontology into Neo4j"""

    driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
    )

    LOGGER.info("Setting uniqueness constraints on nodes")
    with driver.session(database="neo4j") as session:
        for node in NODES:
            session.execute_write(_set_uniqueness_constraints, node)

    LOGGER.info("Loading hospital nodes")
    with open(HOSPITALS_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        with driver.session(database="neo4j") as session:
            for row in reader:
                session.run("""
                    MERGE (h:Hospital {id: toInteger($id), name: $name, state_name: $state})
                """, id=row['hospital_id'], name=row['hospital_name'], state=row['hospital_state'])

    LOGGER.info("Loading payer nodes")
    with open(PAYERS_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        with driver.session(database="neo4j") as session:
            for row in reader:
                session.run("""
                    MERGE (p:Payer {id: toInteger($id), name: $name})
                """, id=row['payer_id'], name=row['payer_name'])

    LOGGER.info("Loading physician nodes")
    with open(PHYSICIANS_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        with driver.session(database="neo4j") as session:
            for row in reader:
                session.run("""
                    MERGE (p:Physician {id: toInteger($id), name: $name, dob: $dob, grad_year: $grad_year, school: $school, salary: toFloat($salary)})
                """, id=row['physician_id'], name=row['physician_name'], dob=row['physician_dob'], 
                     grad_year=row['physician_grad_year'], school=row['medical_school'], salary=row['salary'])

    LOGGER.info("Loading patient nodes")
    with open(PATIENTS_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        with driver.session(database="neo4j") as session:
            for row in reader:
                session.run("""
                    MERGE (p:Patient {id: toInteger($id), name: $name, sex: $sex, dob: $dob, blood_type: $blood_type})
                """, id=row['patient_id'], name=row['patient_name'], sex=row['patient_sex'], 
                     dob=row['patient_dob'], blood_type=row['patient_blood_type'])

    LOGGER.info("Loading visit nodes")
    with open(VISITS_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        with driver.session(database="neo4j") as session:
            for row in reader:
                session.run("""
                    MERGE (v:Visit {id: toInteger($id), room_number: toInteger($room_number), admission_type: $admission_type, 
                                   admission_date: $admission_date, test_results: $test_results, status: $status,
                                   chief_complaint: $chief_complaint, treatment_description: $treatment_description,
                                   diagnosis: $diagnosis, discharge_date: $discharge_date})
                """, id=row['visit_id'], room_number=row['room_number'], admission_type=row['admission_type'],
                     admission_date=row['date_of_admission'], test_results=row['test_results'], status=row['visit_status'],
                     chief_complaint=row['chief_complaint'], treatment_description=row['treatment_description'],
                     diagnosis=row['primary_diagnosis'], discharge_date=row['discharge_date'])

    LOGGER.info("Loading review nodes")
    with open(REVIEWS_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        with driver.session(database="neo4j") as session:
            for row in reader:
                session.run("""
                    MERGE (r:Review {id: toInteger($id), text: $text, patient_name: $patient_name, 
                                    physician_name: $physician_name, hospital_name: $hospital_name})
                """, id=row['review_id'], text=row['review'], patient_name=row['patient_name'],
                     physician_name=row['physician_name'], hospital_name=row['hospital_name'])

    LOGGER.info("Loading 'AT' relationships")
    with open(VISITS_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        with driver.session(database="neo4j") as session:
            for row in reader:
                session.run("""
                    MATCH (v:Visit {id: toInteger($visit_id)})
                    MATCH (h:Hospital {id: toInteger($hospital_id)})
                    MERGE (v)-[:AT]->(h)
                """, visit_id=row['visit_id'], hospital_id=row['hospital_id'])

    LOGGER.info("Loading 'WRITES' relationships")
    with open(REVIEWS_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        with driver.session(database="neo4j") as session:
            for row in reader:
                session.run("""
                    MATCH (v:Visit {id: toInteger($visit_id)})
                    MATCH (r:Review {id: toInteger($review_id)})
                    MERGE (v)-[:WRITES]->(r)
                """, visit_id=row['visit_id'], review_id=row['review_id'])

    LOGGER.info("Loading 'TREATS' relationships")
    with open(VISITS_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        with driver.session(database="neo4j") as session:
            for row in reader:
                session.run("""
                    MATCH (p:Physician {id: toInteger($physician_id)})
                    MATCH (v:Visit {id: toInteger($visit_id)})
                    MERGE (p)-[:TREATS]->(v)
                """, physician_id=row['physician_id'], visit_id=row['visit_id'])

    LOGGER.info("Loading 'COVERED_BY' relationships")
    with open(VISITS_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        with driver.session(database="neo4j") as session:
            for row in reader:
                session.run("""
                    MATCH (v:Visit {id: toInteger($visit_id)})
                    MATCH (p:Payer {id: toInteger($payer_id)})
                    MERGE (v)-[cb:COVERED_BY]->(p)
                    ON CREATE SET cb.service_date = $service_date, cb.billing_amount = toFloat($billing_amount)
                """, visit_id=row['visit_id'], payer_id=row['payer_id'],
                     service_date=row['discharge_date'], billing_amount=row['billing_amount'])

    LOGGER.info("Loading 'HAS' relationships")
    with open(VISITS_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        with driver.session(database="neo4j") as session:
            for row in reader:
                session.run("""
                    MATCH (p:Patient {id: toInteger($patient_id)})
                    MATCH (v:Visit {id: toInteger($visit_id)})
                    MERGE (p)-[:HAS]->(v)
                """, patient_id=row['patient_id'], visit_id=row['visit_id'])

    LOGGER.info("Loading 'EMPLOYS' relationships")
    with open(VISITS_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        with driver.session(database="neo4j") as session:
            for row in reader:
                session.run("""
                    MATCH (h:Hospital {id: toInteger($hospital_id)})
                    MATCH (p:Physician {id: toInteger($physician_id)})
                    MERGE (h)-[:EMPLOYS]->(p)
                """, hospital_id=row['hospital_id'], physician_id=row['physician_id'])

    driver.close()


if __name__ == "__main__":
    load_hospital_graph_from_csv()
