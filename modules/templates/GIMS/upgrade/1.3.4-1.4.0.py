# Database upgrade script
#
# GIMS Template Version 1.3.4 => 1.4.0
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/GIMS/upgrade/1.3.4-1.4.0.py
#
import sys

#from core import S3Duplicate

# Override auth (disables all permission checks)
auth.override = True

# Initialize failed-flag
failed = False

# Info
def info(msg):
    sys.stderr.write("%s" % msg)
def infoln(msg):
    sys.stderr.write("%s\n" % msg)

# Load models for tables
rtable = s3db.s3_permission
rctable = s3db.cr_reception_center
rstable = s3db.cr_reception_center_status

# Paths
IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "GIMS")

# -----------------------------------------------------------------------------
# Migrate capacity numbers
#
if not failed:
    info("Migrate capacity numbers of reception centers")

    updated = 0

    query = (rctable.deleted == False)
    rows = db(query).select(rctable.id,
                            rctable.capacity,
                            rctable.population,
                            rctable.allocatable_capacity,
                            )
    updated = 0
    for row in rows:

        capacity = row.capacity
        population = row.population
        allocatable_capacity = row.allocatable_capacity

        allocable_capacity = allocatable_capacity + population
        if allocable_capacity > capacity:
            capacity = allocable_capacity

        allocable_capacity_estimate = allocable_capacity

        free_capacity = max(0, capacity - population)
        free_allocable_capacity = max(0, allocable_capacity - population)

        occupancy_rate = population * 100 // allocable_capacity if allocable_capacity else 100
        utilization_rate = population * 100 // capacity if capacity else 100

        row.update_record(capacity = capacity,
                          allocable_capacity_estimate = allocable_capacity_estimate,
                          allocable_capacity = allocable_capacity,
                          free_capacity = free_capacity,
                          free_allocable_capacity = free_allocable_capacity,
                          utilization_rate = utilization_rate,
                          occupancy_rate = occupancy_rate,
                          modified_on = rctable.modified_on,
                          modified_by = rctable.modified_by,
                          )
        updated += 1

    infoln("...done (%s records updated)" % updated)

# -----------------------------------------------------------------------------
# Migrate capacity history
#
if not failed:
    info("Migrate capacity history")

    updated = 0

    query = (rstable.deleted == False)
    rows = db(query).select(rstable.id,
                            rstable.capacity,
                            rstable.population,
                            rstable.allocatable_capacity,
                            )
    updated = 0
    for row in rows:

        capacity = row.capacity
        population = row.population
        allocatable_capacity = row.allocatable_capacity

        allocable_capacity = allocatable_capacity + population
        if allocable_capacity > capacity:
            capacity = allocable_capacity

        allocable_capacity_estimate = allocable_capacity

        free_capacity = max(0, capacity - population)
        free_allocable_capacity = max(0, allocable_capacity - population)

        occupancy_rate = population * 100 // allocable_capacity if allocable_capacity else 100
        utilization_rate = population * 100 // capacity if capacity else 100

        row.update_record(capacity = capacity,
                          allocable_capacity = allocable_capacity,
                          allocable_capacity_estimate = allocable_capacity_estimate,
                          free_capacity = free_capacity,
                          free_allocable_capacity = free_allocable_capacity,
                          utilization_rate = utilization_rate,
                          occupancy_rate = occupancy_rate,
                          modified_on = rstable.modified_on,
                          modified_by = rstable.modified_by,
                          )
        updated += 1

    infoln("...done (%s records updated)" % updated)

# -----------------------------------------------------------------------------
# Upgrade user roles
#
if not failed:
    info("Upgrade user roles")

    # Delete invalid rules
    deleted = 0

    query = (rtable.tablename == "cr_reception_center_status")
    deleted +=db(query).delete()

    info("...%s invalid rules removed" % deleted)

    # Re-import correct rules
    bi = s3base.BulkImporter()
    filename = os.path.join(TEMPLATE_FOLDER, "auth_roles.csv")

    try:
        error = bi.import_roles(filename)
    except Exception as e:
        error = sys.exc_info()[1] or "unknown error"
    if error:
        infoln("...failed")
        infoln(error)
        failed = True
    else:
        infoln("...done")

# -----------------------------------------------------------------------------
# Finishing up
#
if failed:
    db.rollback()
    infoln("UPGRADE FAILED - Action rolled back.")
else:
    db.commit()
    infoln("UPGRADE SUCCESSFUL.")

# END =========================================================================
