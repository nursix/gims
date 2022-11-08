# Database upgrade script
#
# GIMS Template Version 1.3.0 => 1.3.1
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/GIMS/upgrade/1.3.0-1.3.1.py
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
sstable = s3db.cr_shelter_status
rstable = s3db.cr_reception_center_status

# Paths
IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "GIMS")

# -----------------------------------------------------------------------------
# Fix realms for shelter status and reception center status
#
if not failed:
    info("Update realms for status records")

    updated = 0

    query = (sstable.realm_entity == None) & \
            (sstable.deleted == False)
    before = db(query).count()
    if before:
        auth.set_realm_entity(sstable, query)
        after = db(query).count()
        updated += (before - after)

    query = (rstable.realm_entity == None) & \
            (rstable.deleted == False)
    before = db(query).count()
    if before:
        auth.set_realm_entity(rstable, query)
        after = db(query).count()
        updated += (before - after)

    infoln("...done (%s records updated)" % updated)

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
