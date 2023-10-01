# Main Migration File
import sys
from DataMigratorToVictoriaMetrics import DataMigration

if __name__ == "__main__":
    metric_name = sys.argv[1]  # Replace this with the metric name you want to migrate
    migration = DataMigration(metric_name)
    migration.run_migration()