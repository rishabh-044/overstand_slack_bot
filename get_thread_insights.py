import requests
from typing import Dict, Any, Optional, List
import pandas as pd
import json
import os 
import dotenv

dotenv.load_dotenv()

def get_all_ontologies(base_url: str, bearer_token: str) -> List[Dict[str, Any]]:
    """
    Fetches a list of all ontologies from the Palantir Foundry API.
    
    Args:
        base_url: The base URL of your Foundry instance (e.g., "https://your-company.palantirfoundry.com")
        bearer_token: The bearer token for authentication
        
    Returns:
        A list of dictionaries containing ontology information
        
    Raises:
        Exception: If the API request fails
    """
    # Construct the API endpoint for fetching all ontologies
    api_url = f"{base_url}/api/v1/ontologies"
    
    # Set up the authorization headers
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # Make the API request
    response = requests.get(api_url, headers=headers)
    
    # Check if the request was successful
    if not response.ok:
        error_message = f"API request failed with status {response.status_code}"
        try:
            error_details = response.json()
            error_message += f": {error_details}"
        except:
            error_message += f": {response.text}"
        
        raise Exception(error_message)
    
    # Parse and return the JSON response
    return response.json()

def get_ontology_object(
    base_url: str,
    ontology_rid: str,
    object_name: str,
    primary_key: str,
    bearer_token: str,
    page_size: int = 1000
) -> List[Dict[str, Any]]:
    """
    Fetch a specific object from a Foundry ontology by its name and primary key.
    
    Args:
        base_url: The base URL of your Foundry instance (e.g., "https://your-company.palantirfoundry.com")
        ontology_rid: The RID of the ontology (e.g., "ri.ontology.main.ontology.12345")
        object_name: The name of the object type to fetch (e.g., "Person", "Organization")
        primary_key: The primary key value of the specific object to fetch
        bearer_token: The bearer token for authentication
        page_size: Number of items to fetch per page (default: 1000)
        
    Returns:
        List of all objects from all pages
        
    Raises:
        Exception: If the API request fails or the object is not found
    """
    all_data = []
    page_token = None
    
    while True:
        # Construct the API endpoint for the specific object
        api_url = f"{base_url}/api/v1/ontologies/{ontology_rid}/objects/{object_name}"
        
        # Set up the authorization headers
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Set up query parameters
        params = {
            "pageSize": page_size
        }
        
        # Add pageToken if we have one (omit for first request)
        if page_token:
            params["pageToken"] = page_token
        
        try:
            # Make the API request
            response = requests.get(api_url, headers=headers, params=params)
            
            # Check if the request was successful
            if not response.ok:
                error_message = f"API request failed with status {response.status_code}"
                try:
                    error_details = response.json()
                    error_message += f": {error_details}"
                except:
                    error_message += f": {response.text}"
                raise Exception(error_message)
            
            # Parse the JSON response
            response_data = response.json()
            
            # Extract data from the response
            if 'data' in response_data:
                page_data = response_data['data']
                if isinstance(page_data, list):
                    all_data.extend(page_data)
                else:
                    all_data.append(page_data)
            
            # Get nextPageToken for the next request
            page_token = response_data.get('nextPageToken')
            if not page_token:
                break
                
            print(f"Retrieved {len(all_data)} objects so far...")
                
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {str(e)}")
    
    print(f"Total objects retrieved: {len(all_data)}")
    return all_data

def list_ontology_objects(base_url: str, ontology_rid: str, bearer_token: str) -> List[Dict[str, Any]]:
    """
    List all objects in a Foundry ontology.
    
    Args:
        base_url: The base URL of your Foundry instance
        ontology_rid: The RID of the ontology
        
    Returns:
        A list of objects in the ontology
        
    Raises:
        Exception: If the API request fails
    """
    # Construct the API endpoint for listing objects
    api_url = f"{base_url}/api/v1/ontologies/{ontology_rid}/objectTypes"
    
    # Set up the authorization headers
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # Make the API request
    response = requests.get(api_url, headers=headers)
    
    # Check if the request was successful
    if not response.ok:
        error_message = f"API request failed with status {response.status_code}"
        try:
            error_details = response.json()
            error_message += f": {error_details}"
        except:
            error_message += f": {response.text}"
        
        raise Exception(error_message)
    
    # Parse and return the JSON response
    return response.json()

def get_object_types(base_url: str, bearer_token: str, object_type: str) -> List[str]:
    """
    Fetches the available object types from the Palantir Foundry API.
    
    Args:
        base_url: The base URL of your Foundry instance
        
    Returns:
        A list of available object types
        
    Raises:
        Exception: If the API request fails
    """
    # Construct the API endpoint for fetching object types
    api_url = f"{base_url}/api/v1/ontologies/{ontology_rid}/{object_type}"
    
    # Set up the authorization headers
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # Make the API request
    response = requests.get(api_url, headers=headers)
    
    # Check if the request was successful
    if not response.ok:
        error_message = f"API request failed with status {response.status_code}"
        try:
            error_details = response.json()
            error_message += f": {error_details}"
        except:
            error_message += f": {response.text}"
        
        raise Exception(error_message)
    
    # Parse and return the JSON response
    return response.json().get("objectTypes", [])

def process_ontology_objects(objects: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Process the ontology objects and convert to a DataFrame.
    
    Args:
        objects: List of ontology objects
        
    Returns:
        A pandas DataFrame containing the ontology objects
    """
    if not objects:
        print("No objects found in this ontology.")
        return pd.DataFrame()
    
    df = pd.DataFrame(objects)
    
    # Display basic information about the data
    print(f"\nFound {len(objects)} objects with {len(df.columns)} attributes.")
    print("\nDataFrame columns:")
    print(df.columns.tolist())
    
    # Display a sample of the data
    print("\nSample data (first 5 rows):")
    print(df.head())
    
    return df

def flatten_json(nested_json: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """
    Flatten a nested JSON structure into a single level dictionary.
    
    Args:
        nested_json: The nested JSON dictionary to flatten
        parent_key: The parent key for nested structures
        sep: Separator to use between keys
        
    Returns:
        A flattened dictionary
    """
    items = {}
    for key, value in nested_json.items():
        # Remove any initial prefix (e.g., "data_", "properties_", etc.)
        clean_key = key.split(sep)[-1] if sep in key else key
        new_key = f"{parent_key}{sep}{clean_key}" if parent_key else clean_key
        
        if isinstance(value, dict):
            items.update(flatten_json(value, new_key, sep=sep))
        elif isinstance(value, list):
            # Convert lists to strings to avoid DataFrame issues
            items[new_key] = json.dumps(value)
        else:
            items[new_key] = value
    return items

def create_attribute_table(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Create a table from JSON data with attributes as columns.
    
    Args:
        data: List of JSON objects
        
    Returns:
        A pandas DataFrame with attributes as columns
    """
    if not data:
        print("No data found to process.")
        return pd.DataFrame()
    
    # Flatten each JSON object
    flattened_data = [flatten_json(item) for item in data]
    
    # Create DataFrame
    insights_df = pd.DataFrame(flattened_data)
    
    # Remove any remaining prefixes from column names
    insights_df.columns = [col.split('_')[-1] if '_' in col else col for col in insights_df.columns]
    
    # Display information about the data
    print(f"\nProcessed {len(data)} objects")
    print(f"Total columns: {len(insights_df.columns)}")
    print("\nColumns in the table:")
    for col in insights_df.columns:
        print(f"- {col}")
    
    return insights_df

def get_recent_data(df: pd.DataFrame, timestamp_col: str, last_updated_at: str) -> pd.DataFrame:
    """
    Filter DataFrame to get only recent data based on timestamp.
    
    Args:
        df: Input DataFrame containing the data
        timestamp_col: Name of the column containing timestamps
        last_updated_at: ISO format timestamp string (e.g., "2024-03-20T10:00:00Z")
        
    Returns:
        Filtered DataFrame containing only recent data
        
    Raises:
        ValueError: If timestamp_col doesn't exist or last_updated_at is invalid
    """
    if timestamp_col not in df.columns:
        raise ValueError(f"Timestamp column '{timestamp_col}' not found in DataFrame")
    
    try:
        # Convert last_updated_at to datetime using ISO8601 format
        cutoff_time = pd.to_datetime(last_updated_at, format='ISO8601')
        
        # Convert timestamp column to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
            # Handle potential string timestamps with milliseconds and timezone
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], format='ISO8601')
        
        # Filter data
        recent_data = df[df[timestamp_col] >= cutoff_time]
        
        print(f"\nFound {len(recent_data)} records updated after {last_updated_at}")
        return recent_data
        
    except Exception as e:
        raise ValueError(f"Error processing timestamps: {str(e)}")

# Example usage
if __name__ == "__main__":
    foundry_url = os.getenv("FOUNDRY_URL")
    ontology_rid = os.getenv("ONTOLOGY_RID")
    object_type = "ThreadInsight"
    primary_key = "internalInsightId"
    token = os.getenv("FOUNDRY_BEARER_TOKEN")
    try:
        result = get_ontology_object(
            base_url=foundry_url,
            bearer_token=token,
            ontology_rid=ontology_rid,
            object_name=object_type,
            primary_key=primary_key
        )
        print("Data retrieved successfully")
        
        # Create attribute table from the data
        if isinstance(result, list):
            df = create_attribute_table(result)
            
            # Example: Get data updated after a specific timestamp
            last_updated_at = "2024-04-30T10:00:00Z"  # Replace with your desired timestamp
            timestamp_col = "timestamp"  # Replace with your actual timestamp column name
            
            recent_data = get_recent_data(df, timestamp_col, last_updated_at)
            print("\nRecent data:")
            print(recent_data)
        else:
            print("Data is not in the expected format (list of objects)")
            
    except Exception as e:
        print(f"Error: {e}")

