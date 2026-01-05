import random
import datetime
import string

# Function to generate random string for names, codes, etc.
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Fixed lists for realism
genders = ['M', 'F']
encounter_types = ['Outpatient', 'Inpatient', 'ER']

# Generate fixed specialties (10)
specialties = [
    (i+1, f'Specialty_{i+1}', random_string(4).upper()) for i in range(10)
]

# Generate fixed departments (20)
departments = [
    (i+1, f'Department_{i+1}', random.randint(1, 10), random.randint(10, 50)) for i in range(20)
]

# Generate fixed providers (100), assign random specialty and department
providers = []
for i in range(100):
    specialty_id = random.choice(specialties)[0]
    department_id = random.choice(departments)[0]
    providers.append(
        (i+101, random_string(8), random_string(10), 'MD', specialty_id, department_id)
    )

# Generate fixed patients (2000, to have repeats)
patients = []
for i in range(2000):
    dob = datetime.date(random.randint(1920, 2020), random.randint(1,12), random.randint(1,28))
    patients.append(
        (i+1001, random_string(8), random_string(10), dob, random.choice(genders), f'MRN{random_string(5)}')
    )

# Generate fixed diagnoses (50)
diagnoses = [
    (i+3001, random_string(3).upper(), f'Description_{i+1}') for i in range(50)
]

# Generate fixed procedures (50)
procedures = [
    (i+4001, random_string(5), f'Proc_Description_{i+1}') for i in range(50)
]

# Function to chunk list for batched inserts
def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# Generate encounters and related
encounters = []
encounter_diagnoses = []
encounter_procedures = []
billings = []

start_date = datetime.datetime(2020, 1, 1)
enc_diag_id = 8001
enc_proc_id = 9001
for i in range(10000):
    encounter_id = i + 7001
    patient = random.choice(patients)
    provider = random.choice(providers)
    enc_type = random.choice(encounter_types)
    enc_date = start_date + datetime.timedelta(days=random.randint(0, 1825))  # Up to 5 years
    duration_hours = random.randint(1, 72) if enc_type == 'Inpatient' else random.randint(1, 4)
    discharge_date = enc_date + datetime.timedelta(hours=duration_hours)
    department_id = provider[5]  # From provider's department
    
    encounters.append(
        (encounter_id, patient[0], provider[0], enc_type, enc_date.strftime('%Y-%m-%d %H:%M:%S'), discharge_date.strftime('%Y-%m-%d %H:%M:%S'), department_id)
    )
    
    # 1-5 diagnoses per encounter
    num_diag = random.randint(1, 5)
    for j in range(num_diag):
        diag = random.choice(diagnoses)
        encounter_diagnoses.append(
            (enc_diag_id, encounter_id, diag[0], j+1)
        )
        enc_diag_id += 1
    
    # 0-3 procedures per encounter
    num_proc = random.randint(0, 3)
    for j in range(num_proc):
        proc = random.choice(procedures)
        proc_date = enc_date.date() + datetime.timedelta(days=random.randint(0, duration_hours//24))
        encounter_procedures.append(
            (enc_proc_id, encounter_id, proc[0], proc_date.strftime('%Y-%m-%d'))
        )
        enc_proc_id += 1
    
    # 1 billing per encounter
    billing_id = i + 14001
    claim_amount = round(random.uniform(100, 20000), 2)
    allowed_amount = round(claim_amount * random.uniform(0.5, 0.9), 2)
    claim_date = discharge_date.date() + datetime.timedelta(days=random.randint(1, 30))
    status = random.choice(['Paid', 'Pending', 'Denied'])
    billings.append(
        (billing_id, encounter_id, claim_amount, allowed_amount, claim_date.strftime('%Y-%m-%d'), status)
    )

# Now, write to file
with open('load_data.sql', 'w') as f:
    # Helper function to write batched insert with SQL-safe literals
    def write_batched_insert(table, columns, data, batch_size=500):
        def sql_literal(val):
            if val is None:
                return 'NULL'
            if isinstance(val, str):
                return "'" + val.replace("'", "''") + "'"
            if isinstance(val, bool):
                return '1' if val else '0'
            if isinstance(val, (int, float)):
                return str(val)
            if isinstance(val, datetime.datetime):
                return "'" + val.strftime('%Y-%m-%d %H:%M:%S') + "'"
            if isinstance(val, datetime.date):
                return "'" + val.strftime('%Y-%m-%d') + "'"
            return "'" + str(val).replace("'", "''") + "'"

        f.write(f'-- Insert into {table}\n')
        for chunk in chunk_list(data, batch_size):
            values = ',\n'.join([f"({', '.join([sql_literal(val) for val in row])})" for row in chunk])
            f.write(f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n{values};\n\n")
    
    # Specialties
    write_batched_insert(
        'specialties',
        ['specialty_id', 'specialty_name', 'specialty_code'],
        specialties,
        100
    )
    
    # Departments
    write_batched_insert(
        'departments',
        ['department_id', 'department_name', 'floor', 'capacity'],
        departments,
        100
    )
    
    # Providers
    write_batched_insert(
        'providers',
        ['provider_id', 'first_name', 'last_name', 'credential', 'specialty_id', 'department_id'],
        providers,
        100
    )
    
    # Patients
    write_batched_insert(
        'patients',
        ['patient_id', 'first_name', 'last_name', 'date_of_birth', 'gender', 'mrn'],
        patients,
        500
    )
    
    # Diagnoses
    write_batched_insert(
        'diagnoses',
        ['diagnosis_id', 'icd10_code', 'icd10_description'],
        diagnoses,
        100
    )
    
    # Procedures
    write_batched_insert(
        'procedures',
        ['procedure_id', 'cpt_code', 'cpt_description'],
        procedures,
        100
    )
    
    # Encounters
    write_batched_insert(
        'encounters',
        ['encounter_id', 'patient_id', 'provider_id', 'encounter_type', 'encounter_date', 'discharge_date', 'department_id'],
        encounters,
        500
    )
    
    # Encounter diagnoses
    write_batched_insert(
        'encounter_diagnoses',
        ['encounter_diagnosis_id', 'encounter_id', 'diagnosis_id', 'diagnosis_sequence'],
        encounter_diagnoses,
        1000
    )
    
    # Encounter procedures
    write_batched_insert(
        'encounter_procedures',
        ['encounter_procedure_id', 'encounter_id', 'procedure_id', 'procedure_date'],
        encounter_procedures,
        1000
    )
    
    # Billing
    write_batched_insert(
        'billing',
        ['billing_id', 'encounter_id', 'claim_amount', 'allowed_amount', 'claim_date', 'claim_status'],
        billings,
        500
    )

print("SQL insert script generated and saved to 'inserts.sql'. You can run this file in your MySQL database to populate the tables.")