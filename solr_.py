import requests
import pandas as pd
import json

# Constants
SOLR_URL = 'http://localhost:8989/solr'

def createCollection(collection_name):
    response = requests.get(f'{SOLR_URL}/admin/collections?action=CREATE&name={collection_name}&replicationFactor=1')
    print(f"Create Collection Response: {response.json()}")

def indexData(collection_name, exclude_column):
    # Load employee data
    try:
        df = pd.read_csv('C:\\Users\\swathika\\OneDrive\\Documents\\Employee Sample Data 1.csv')
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return
    
    # Drop the specified column
    if exclude_column in df.columns:
        df = df.drop(columns=[exclude_column])
    
    # Convert DataFrame to JSON format
    data_json = df.to_json(orient='records')
    data = json.loads(data_json)

    # Index data
    response = requests.post(f'{SOLR_URL}/{collection_name}/update?commit=true', json=data)
    print(f"Index Data Response: {response.json()}")

def getEmpCount(collection_name):
    response = requests.get(f'{SOLR_URL}/{collection_name}/select?q=*:*&rows=0')
    print(f"Response from getEmpCount: {response.json()}")  # Print full response
    print(f"Employee Count: {response.json().get('response', {}).get('numFound', 'Error retrieving count')}")

def delEmpById(collection_name, employee_id):
    payload = {'delete': {'id': employee_id}}
    response = requests.post(f'{SOLR_URL}/{collection_name}/update?commit=true', json=payload)
    print(f"Delete Employee By ID Response: {response.json()}")

def searchByColumn(collection_name, column_name, column_value):
    response = requests.get(f'{SOLR_URL}/{collection_name}/select?q={column_name}:{column_value}')
    print(f"Search By Column Response: {response.json()}")

def getDepFacet(collection_name):
    response = requests.get(f'{SOLR_URL}/{collection_name}/select?q=*:*&facet=true&facet.field=Department')
    print(f"Department Facet Response: {response.json()}")

if __name__ == "__main__":
    # Define collection names
    v_nameCollection = 'Hash_Nageshwari'
    v_phoneCollection = 'Hash_8807'
    
    # Create collections
    createCollection(v_nameCollection)
    createCollection(v_phoneCollection)

    # Get employee count after collection creation
    getEmpCount(v_nameCollection)

    # Index data while excluding specified columns
    indexData(v_nameCollection, 'Department')
    indexData(v_phoneCollection, 'Gender')

    # Get employee count after indexing
    getEmpCount(v_nameCollection)

    # Delete an employee by ID
    delEmpById(v_nameCollection, 'E02003')

    # Get employee count after deletion
    getEmpCount(v_nameCollection)

    # Search by column
    searchByColumn(v_nameCollection, 'Department', 'IT')
    searchByColumn(v_nameCollection, 'Gender', 'Male')
    searchByColumn(v_phoneCollection, 'Department', 'IT')

    # Get department facet
    getDepFacet(v_nameCollection)
    getDepFacet(v_phoneCollection)

