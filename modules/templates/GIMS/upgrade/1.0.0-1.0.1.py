# Database upgrade script
#
# GIMS Template Version 1.0.0 => 1.0.1
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/GIMS/upgrade/1.0.0-1.0.1.py
#
import sys

#from core import S3Duplicate

# Override auth (disables all permission checks)
auth.override = True

# Failed-flag
failed = False

# Info
def info(msg):
    sys.stderr.write("%s" % msg)
def infoln(msg):
    sys.stderr.write("%s\n" % msg)

# Load models for tables
otable = s3db.org_organisation
gtable = s3db.org_group
mtable = s3db.org_group_membership
ttable = s3db.org_organisation_tag

IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "GIMS")

# -----------------------------------------------------------------------------
# Fix role assignments
#
if not failed:
    info("Fix role assignments")

    table = auth.settings.table_membership
    updated = db(table.id > 0).update(system=False)
    infoln("...done (%s records updated)" % updated)

# -----------------------------------------------------------------------------
# Generate DistrictID tags
#
if not failed:
    info("Generate DistrictID tags ")

    from templates.GIMS.config import DISTRICTS, COMMUNES

    join = [mtable.on((mtable.organisation_id == otable.id) & \
                      (mtable.deleted == False)),
            gtable.on((gtable.id == mtable.group_id) & \
                      ((gtable.name == DISTRICTS) | (gtable.name.like("%s%%" % COMMUNES)))),
            ]
    left = [ttable.on((ttable.organisation_id == otable.id) & \
                      (ttable.tag == "DistrictID") & \
                      (ttable.deleted == False)),
            ]
    query = (otable.deleted == False)
    rows = db(query).select(otable.id,
                            otable.acronym,
                            ttable.id,
                            join = join,
                            left = left,
                            )

    added = 0
    for row in rows:
        org = row.org_organisation
        district_tag = row.org_organisation_tag

        if district_tag.id or not org.acronym:
            info(".")
            continue

        district_id = org.acronym[3:6]
        if len(district_id) == 3 and district_id.isdigit():
            ttable.insert(organisation_id = org.id,
                          tag = "DistrictID",
                          value = district_id,
                          )
            info("+")
            added += 1
        else:
            info(".")

    infoln("...done (%s tags added)" % added)

# -----------------------------------------------------------------------------
# Generate District org groups
#
if not failed:
    info("Generate and Assign District Groups")

    join = [mtable.on((mtable.organisation_id == otable.id) & \
                      (mtable.deleted == False)),
            gtable.on((gtable.id == mtable.group_id) & \
                      (gtable.name.like("%s%%" % COMMUNES))),
            ttable.on((ttable.organisation_id == otable.id) & \
                      (ttable.tag == "DistrictID") & \
                      (ttable.deleted == False)),
            ]
    query = (otable.deleted == False)
    rows = db(query).select(otable.id,
                            mtable.id,
                            mtable.group_id,
                            ttable.value,
                            join = join,
                            )

    groups = {}
    created = linked = 0
    for row in rows:
        info(".")
        district_tag = row.org_organisation_tag
        group_name = "%s (%s)" % (COMMUNES, district_tag.value)

        group_id = groups.get(group_name)
        if not group_id:
            query = (gtable.name == group_name)
            group = db(query).select(gtable.id, gtable.deleted, limitby=(0, 1)).first()
            if group:
                group_id = group.id
                if group.deleted:
                    group.update_record(deleted=False, deleted_fk=None)
            else:
                group = {"name": group_name}
                group_id = group["id"] = gtable.insert(**group)
                s3db.update_super(gtable, group)
                auth.s3_set_record_owner(gtable, group_id)
                s3db.onaccept(gtable, group, method="create")
                created += 1

        membership = row.org_group_membership
        if membership.group_id != group_id:
            # Update it
            membership.update_record(group_id=group_id)
            s3db.onaccept(mtable, membership)
            linked += 1

    infoln("...done (%s groups created, %s organisations linked)" % (created, linked))

# -----------------------------------------------------------------------------
# Finishing up
#
if failed:
    db.rollback()
    infoln("UPGRADE FAILED - Action rolled back.")
else:
    db.commit()
    infoln("UPGRADE SUCCESSFUL.")
