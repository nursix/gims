# Database upgrade script
#
# GIMS Template Version 1.5.0 => 1.6.0
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/GIMS/upgrade/1.5.0-1.6.0.py
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
    sys.stderr.flush()
def infoln(msg):
    sys.stderr.write("%s\n" % msg)
    sys.stderr.flush()

# Load models for tables
otable = s3db.org_organisation
dtable = s3db.cms_newsletter_distribution
rtable = s3db.s3_permission

# Paths
IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "GIMS")

settings.base.debug = True

# -----------------------------------------------------------------------------
# Rename default organisation
#
if not failed:
    info("Rename default organisation")

    OLD = "Ministerium für Familie, Frauen, Kultur und Integration"
    NEW = "Ministerium des Innern, für Integration und Verkehr"

    query = ((otable.name == NEW) | (otable.name == OLD)) & (otable.deleted == False)
    rows = db(query).select(otable.id,
                            otable.name,
                            otable.acronym,
                            otable.logo,
                            )

    if len(rows) == 1:
        organisation = rows.first()
        if organisation.name == OLD:
            data = {"name": NEW, "acronym": "MDI"}
            path = f"{request.folder}/static/themes/RLP/img/logo_mdi.png"
            with open(path) as logo_file:
                data["logo"] = otable.logo.store(logo_file)
            organisation.update_record(**data)
            infoln("...done")
        else:
            infoln("...already done, skipping")
    elif len(rows) == 2:
        failed = True
        infoln("...failed (duplicate organisation found)")
    else:
        failed = True
        infoln("...failed (organisation not found)")

# -----------------------------------------------------------------------------
# Migrate saved filters
#
if not failed:
    info("Migrate saved filters")

    uftable = s3db.table("usr_filter")
    pftable = s3db.table("pr_filter")

    if uftable and not pftable:

        # Re-instate pr_filter model
        db.define_table("pr_filter",
                        s3db.super_link("pe_id", "pr_pentity"),
                        Field("title"),
                        Field("controller"),
                        Field("function"),
                        Field("resource"),
                        Field("url"),
                        Field("description", "text"),
                        Field("query", "text"),
                        Field("serverside", "json"),
                        Field("comments", "text"),
                        *MetaFields(),
                        migrate = True,
                        fake_migrate = True,
                        )
        pftable = db.pr_filter

        try:
            # Select all pr_filter that have not yet been migrated
            left = uftable.on(uftable.uuid == pftable.uuid)
            query = (pftable.id > 0) & (uftable.id == None)
            rows = db(query).select(pftable.ALL, left=left, orderby=pftable.id)
        except Exception:
            infoln("...legacy table does no longer exist or incompatible model, skipping")
        else:
            # Migrate the filters
            migrated = 0
            for row in rows:
                record = {fn:row[fn] for fn in uftable.fields if fn in row and fn != "id"}
                saved_filter_id = uftable.insert(**record)
                migrated += 1
                info("+")
            infoln("...done (%s filters migrated)" % migrated)

        # Drop the .table file for pr_filter
        db._adapter._drop_table_cleanup(pftable)

    elif pftable:
        infoln("...legacy model still active, skipping")
    else:
        infoln("...target model not found, skipping")

# -----------------------------------------------------------------------------
# Cleanup newsletter distribution lists
#
if not failed:
    info("Clean up newsletter distribution lists")

    query = (dtable.saved_filter_id == None) & \
            (dtable.deleted == False)
    deleted = db(query).delete()

    infoln("...done (%s records removed)" % deleted)

# -----------------------------------------------------------------------------
# Upgrade user roles
#
if not failed:
    info("Upgrade user roles")

    # Delete invalid rules
    query = (rtable.tablename.belongs({"pr_filter"}))
    deleted = db(query).delete()
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
