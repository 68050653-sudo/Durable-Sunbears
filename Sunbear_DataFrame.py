class DataFrame:
    """
    DataFrame is a class that represents a table-like data structure
    in Python.
    """

    """
    TODO(P1): Describe your rational for the file format you will use to
        store a DataFrame object.
    """
import json     # import module JSON format for persist to disk
import os       # import modul OS for check Pathname 
    
def __init__(
        self,
        column_names: list[str] = None,
        column_types: list[type] = None,
        rows: list[tuple] = None,
    ):
        """
        TODO(P1): 
        - Modify this constructor as needed
        - Write a docstring for this constructor
          (You should remove this TODO(P1): and replace with your docstring)
        """
# Method 1 ==================================================================================        
        self.column_names = column_names  
        self.column_types = column_types  
        self.rows = []   
        self.removed_indices = set()   # Method 3                
    
def __str__(self):
        # Filter for row not removed   <== Method 3 
        active_rows = [row for i, row in enumerate(self.rows) 
                       if i not in self.removed_indices]
        if len(active_rows) == 0:  # Method 3
            result = "\n" + "="*70 + "\n"
            result += "DataFrame (Empty)\n"
            result += "="*70 + "\n"
            result += f"Columns: {', '.join(self.column_names)}\n"
            result += f"Types: {', '.join([t.__name__ for t in self.column_types])}\n"
            if len(self.rows) > 0:
                result += f"Note: {len(self.removed_indices)} row(s) have been removed\n"
            result += "="*70 + "\n"
            return result
        
        col_widths = []
        for i, col_name in enumerate(self.column_names):
            max_width = len(col_name)
            for row in active_rows: # Method 3
                max_width = max(max_width, len(str(row[i])))
            col_widths.append(max_width + 2)
        
        total_width = sum(col_widths) + len(self.column_names) + 1
        separator = "=" * total_width
        
        header = "|"
        for i, col_name in enumerate(self.column_names):
            header += f" {col_name:<{col_widths[i]-1}}|"
        
        type_row = "|"
        for i, col_type in enumerate(self.column_types):
            type_name = f"({col_type.__name__})"
            type_row += f" {type_name:<{col_widths[i]-1}}|"
        
        mid_separator = "-" * total_width
        
        data_rows = []
        for row in active_rows:  # Method 3
            row_str = "|"
            for i, value in enumerate(row):
                row_str += f" {str(value):<{col_widths[i]-1}}|"
            data_rows.append(row_str)
        
        result = "\n" + separator + "\n"
        result += header + "\n"
        result += type_row + "\n"
        result += mid_separator + "\n"
        result += "\n".join(data_rows) + "\n"
        result += separator + "\n"
        # - STATISTICS
        result += f"Active Rows: {len(active_rows)} | "
        result += f"Removed Rows: {len(self.removed_indices)} | "
        result += f"Total Columns: {len(self.column_names)}\n"
        result += separator + "\n"
        
        return result
    
def insert(self, row):
        """
        Method insert row to DataFrame
        Parameters:
        -----------
        row : list or tuple
        Raises:
        -------
        ValueError
        TypeError
        """
        # Check row have elements = Column
        if len(row) != len(self.column_names):
            raise ValueError(
                f"Row length mismatch: expected {len(self.column_names)} columns, "
                f"but got {len(row)} elements"
            )
        
        # Check type Values in row
        for i, (value, expected_type) in enumerate(zip(row, self.column_types)):
            if not isinstance(value, expected_type):
                raise TypeError(
                    f"Type mismatch in column '{self.column_names[i]}': "
                    f"expected {expected_type.__name__}, "
                    f"but got {type(value).__name__} (value: {value})"
                )
        
        # Pass --> Insert to rows
        self.rows.append(list(row))  
        print(f"/ Row inserted successfully! Total rows: {len(self.rows)}")

# Method 2 ===========================================================================================  

def remove(self, index):
        """
        Method for Delete row form DataFrame in logical delete
        Parameters:
        -----------
        index : int
        Raises:
        -------
        IndexError
        ValueError

        """
        # check index is TRUE
        if index < 0 or index >= len(self.rows):
            raise IndexError(
                f"Index out of range: index {index} is invalid. "
                f"Valid range is 0 to {len(self.rows)-1}"
            )
        
        # check row to Remove
        if index in self.removed_indices:
            raise ValueError(
                f"Row at index {index} has already been removed"
            )
        
        # Mark (logical delete)
        self.removed_indices.add(index)
        print(f"✓ Row at index {index} marked as removed")
    
def get_active_rows(self):
        return [(i, row) for i, row in enumerate(self.rows) 
                if i not in self.removed_indices]
    
def count_active_rows(self):
        """
        Helper method Count Row active
        
        Returns:
        --------
        int : Amount row active
        """
        return len(self.rows) - len(self.removed_indices)
# Method 4 ================================================================================
def persist_to_disk(self, file_path):
        """
        Method save DataFrame to file
        """
        # chek directory 
        directory = os.path.dirname(file_path)
        
        # True > directory (not current directory)
        if directory and not os.path.exists(directory):
            raise FileNotFoundError(
                f"Directory does not exist: '{directory}'. "
                f"Please create the directory before saving the file."
            )
        
        # filter not include removed rows
        active_rows = [row for i, row in enumerate(self.rows) 
                       if i not in self.removed_indices]
        
        # Create to JSON format
        data_structure = {
            "metadata": {
                "column_names": self.column_names,
                "column_types": [t.__name__ for t in self.column_types],
                "total_rows": len(active_rows),
                "removed_count": len(self.removed_indices),
                "file_format_version": "1.0",
                "description": "Sunbears DataFrame serialized data"
            },
            "data": active_rows  # for active
        }
        
        # Write to file in fomat JSON
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data_structure, f, indent=2, ensure_ascii=False)
            
            print(f"✓ DataFrame saved successfully to '{file_path}'")
            print(f"  - Saved {len(active_rows)} active rows")
            print(f"  - Excluded {len(self.removed_indices)} removed rows")
            print(f"  - File size: {os.path.getsize(file_path)} bytes")
            
        except Exception as e:
            raise IOError(f"Failed to write file '{file_path}': {str(e)}")
# Method 5 ================================================================================
@staticmethod
def load_from_disk(file_path):
        """
        Static method Load DataFrame form File 
         Parameters:
        -----------
        file_path : str
        Returns:
        --------
        DataFrame : DataFrame object reconstruct         
        Raises:
        -------
        FileNotFoundError
        ValueError
        """
        # check files 
        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"File not found: '{file_path}'. "
                f"Please check if the file exists."
            )
        
        # read & parse JSON file
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data_structure = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Invalid JSON format in file '{file_path}': {str(e)}. "
                f"This file may not be a valid Sunbears DataFrame file."
            )
        except Exception as e:
            raise IOError(f"Failed to read file '{file_path}': {str(e)}")
        
        # check Signature 
        if "signature" not in data_structure:
            raise ValueError(
                f"Missing signature in file '{file_path}'. "
                f"This file was not created by persist_to_disk() method."
            )
        
        if data_structure["signature"] != "SUNBEARS_DATAFRAME_V1":
            raise ValueError(
                f"Invalid signature: '{data_structure['signature']}'. "
                f"Expected 'SUNBEARS_DATAFRAME_V1'. "
                f"This file may not be a valid Sunbears DataFrame file."
            )
        
        # check metadata & data 
        if "metadata" not in data_structure:
            raise ValueError(
                f"Missing 'metadata' in file '{file_path}'. "
                f"File format is invalid."
            )
        
        if "data" not in data_structure:
            raise ValueError(
                f"Missing 'data' in file '{file_path}'. "
                f"File format is invalid."
            )
        
        metadata = data_structure["metadata"]
        
        # metadata is correct
        required_fields = ["column_names", "column_types"]
        for field in required_fields:
            if field not in metadata:
                raise ValueError(
                    f"Missing '{field}' in metadata. "
                    f"File format is invalid."
                )
        
        # convert type names (string) to type objects
        type_mapping = {
            'str': str,
            'int': int,
            'float': float,
            'bool': bool
        }
        
        try:
            column_types = []
            for type_name in metadata["column_types"]:
                if type_name not in type_mapping:
                    raise ValueError(
                        f"Unknown type: '{type_name}'. "
                        f"Supported types: {list(type_mapping.keys())}"
                    )
                column_types.append(type_mapping[type_name])
        except Exception as e:
            raise ValueError(f"Failed to parse column types: {str(e)}")
        
        # create DataFrame object 
        df = DataFrame(
            column_names=metadata["column_names"],
            column_types=column_types
        )
        
        # Load data to  DataFrame
        data_rows = data_structure["data"]
        
        if "total_rows" in metadata and len(data_rows) != metadata["total_rows"]:
            print(f"⚠️  Warning: Data rows count ({len(data_rows)}) does not match metadata ({metadata['total_rows']})")
        
        for i, row in enumerate(data_rows):
            try:
                df.insert(row)
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"Failed to insert row {i} from file: {str(e)}. "
                    f"Row data: {row}"
                )
        
        print(f"✓ DataFrame loaded successfully from '{file_path}'")
        print(f"  - Loaded {len(data_rows)} rows")
        print(f"  - Columns: {len(df.column_names)}")
        
        return df
