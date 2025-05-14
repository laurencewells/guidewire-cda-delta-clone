from guidewire.processor import Processor
from typing import Tuple

# Define table names
TABLE_NAMES: Tuple[str, ...] = (
    "policy_holders",
    "policy_holders2",
    "policy_holders3",
    "policy_holders4",
)

def main() -> None:
    """Main entry point for the application."""
    processor = Processor(table_names=TABLE_NAMES, parallel=False)
    processor.run()

if __name__ == "__main__":
    main()


## Read all tables from manfiest, accept the table names as an arguement 
