"""
    RLPPTM Test Station Management Extensions

    Copyright: 2022 (c) AHSS

    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation
    files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following
    conditions:

    The above copyright notice and this permission notice shall be
    included in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
    OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
    HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
    OTHER DEALINGS IN THE SOFTWARE.
"""

__all__ = ("TestProviderModel",
           "TestStationModel"
           )

from gluon import current, Field, URL, IS_EMPTY_OR, IS_IN_SET, DIV, I
from gluon.storage import Storage

from core import DataModel, \
                 get_form_record_id, represent_option, s3_comments, \
                 s3_comments_widget, s3_date, s3_datetime, s3_meta_fields, \
                 s3_text_represent, s3_yes_no_represent

DEFAULT = lambda: None

# =============================================================================
class TestProviderModel(DataModel):
    """
        Data model extensions for test provider verification and commissioning
    """

    names = ("org_verification",
             "org_commission",
             )

    def model(self):

        T = current.T

        organisation_id = self.org_organisation_id

        configure = self.configure
        define_table = self.define_table

        # ---------------------------------------------------------------------
        mgrinfo_opts = (("N/A", T("not specified")),
                        ("ACCEPT", T("not required")),
                        ("REVISE", T("Completion/Adjustment required")),
                        ("COMPLETE", T("complete")),
                        )

        commission_status_reasons = (("N/V", T("Verification Pending")),
                                     ("OVERRIDE", T("Set by Administrator")),
                                     )

        # ---------------------------------------------------------------------
        # Verification details
        #
        orgtype_opts = self.orgtype_status_opts

        tablename = "org_verification"
        define_table(tablename,
                     organisation_id(),
                     # Hidden data hash to detect relevant changes
                     Field("dhash",
                           readable = False,
                           writable = False,
                           ),
                     # Whether organisation type is verified
                     Field("orgtype",
                           label = T("Type Verification"),
                           default = "N/A",
                           requires = IS_IN_SET(orgtype_opts(selectable=True),
                                                sort = False,
                                                zero = None,
                                                ),
                           represent = workflow_tag_represent(dict(orgtype_opts())),
                           readable = True,
                           writable = False,
                           ),
                     # Whether manager information is complete and verified
                     Field("mgrinfo",
                           label = T("Test Station Manager Information"),
                           default = "N/A",
                           requires = IS_IN_SET(mgrinfo_opts, zero=None),
                           represent = workflow_tag_represent(dict(mgrinfo_opts)),
                           readable = True,
                           writable = False,
                           ),
                     # Overall accepted-status
                     Field("accepted", "boolean",
                           label = T("Verified"),
                           default = False,
                           represent = s3_yes_no_represent,
                           readable = False,
                           writable = False,
                           ),
                     *s3_meta_fields())

        # ---------------------------------------------------------------------
        # Commission
        #
        status_opts = self.commission_status_opts

        tablename = "org_commission"
        define_table(tablename,
                     organisation_id(empty=False),
                     s3_date(default = "now",
                             past = 0,
                             set_min="#org_commission_end_date",
                             ),
                     s3_date("end_date",
                             default = None,
                             label = T("expires on"),
                             set_max="#org_commission_date",
                             ),
                     # TODO color-coded representation
                     Field("status",
                           default = "CURRENT",
                           requires = IS_IN_SET(status_opts(selectable=True),
                                                zero = None,
                                                sort = False,
                                                ),
                           represent = represent_option(dict(status_opts())),
                           readable = True,
                           writable = False,
                           ),
                     Field("prev_status",
                           readable = False,
                           writable = False,
                           ),
                     s3_date("status_date",
                             label = T("Status updated on"),
                             writable = False,
                             ),
                     Field("status_reason",
                           requires = IS_EMPTY_OR(
                                        IS_IN_SET(commission_status_reasons,
                                                  sort = False,
                                                  )),
                           represent = workflow_tag_represent(dict(commission_status_reasons)),
                           ),
                     s3_comments(),
                     *s3_meta_fields())

        # Table configuration
        configure(tablename,
                  insertable = False,
                  editable = False,
                  deletable = False,
                  onvalidation = self.commission_onvalidation,
                  onaccept = self.commission_onaccept,
                  # TODO default orderby start date (newest first)
                  )

        # TODO CRUD strings

    #--------------------------------------------------------------------------
    @staticmethod
    def orgtype_status_opts(selectable=False):
        """
            Status options for organisation type verification

            Args:
                selectable: the selectable options
                            - a list of status codes
                            - True for default selectable options
                            - False for all options

            Returns:
                tuple|list of (code, label)
        """

        T = current.T

        opts =(("N/A", T("not specified")),
               ("ACCEPT", T("not required")),
               ("N/V", T("not verified")),
               ("VERIFIED", T("verified")),
               )
        if not selectable:
            return opts
        elif selectable is True:
            # Default selectable options
            return [o for o in opts if o in ("N/V", "VERIFIED")]
        else:
            return [o for o in opts if o in selectable]

    #--------------------------------------------------------------------------
    @staticmethod
    def commission_status_opts(selectable=False):
        """
            Status options for commissions

            Args:
                selectable: the selectable options
                            - a list of status codes
                            - True for default selectable options
                            - False for all options

            Returns:
                tuple|list of (code, label)
        """

        T = current.T
        opts = (("CURRENT", T("current")),
                ("SUSPENDED", T("suspended")),
                ("REVOKED", T("revoked")),
                ("EXPIRED", T("expired")),
                )
        if not selectable:
            return opts
        elif selectable is True:
            # Default selectable options
            return [o for o in opts if o[0] in ("CURRENT", "SUSPENDED", "REVOKED")]
        else:
            return [o for o in opts if o[0] in selectable]

    #--------------------------------------------------------------------------
    @staticmethod
    def commission_onvalidation(form):
        """
            Onvalidation of commission form:
                - make sure end date is after start date
                - prevent overlapping commissions
                - validate status
                - require reason for SUSPENDED-status
        """

        T = current.T
        db = current.db
        s3db = current.s3db

        record_id = get_form_record_id(form)
        ctable = s3db.org_commission

        # Get record data
        form_vars = form.vars
        data = {}
        load = []
        for fn in ("organisation_id", "date", "end_date", "status"):
            if fn in form_vars:
                data[fn] = form_vars[fn]
            else:
                data[fn] = ctable[fn].default
                load.append(fn)
        if load and record_id:
            record = db(ctable.id == record_id).select(*load, limitby=(0, 1)).first()
            for fn in load:
                data[fn] = record[fn]

        organisation_id = data["organisation_id"]
        start = data["date"]
        end = data["end_date"]
        status = data["status"]

        if "end_date" in form_vars:
            # End date must be after start date
            if start and end and end <= start:
                form.errors["end_date"] = T("End date must be after start date")
                return

        active_statuses = ("CURRENT", "SUSPENDED")

        if status in active_statuses:
            # Prevent overlapping active commissions
            query = (ctable.status.belongs(active_statuses)) & \
                    (ctable.organisation_id == organisation_id) & \
                    ((ctable.end_date == None) | (ctable.end_date >= start))
            if record_id:
                query = (ctable.id != record_id) & query
            if end:
                query &= (ctable.date <= end)
            query &= (ctable.deleted == False)
            row = db(query).select(ctable.id, limitby=(0, 1)).first()
            if row:
                error = T("Date interval overlaps existing commission")
                if "date" in form_vars:
                    form.errors["date"] = error
                if "end_date" in form_vars:
                    form.errors["end_date"] = error
                if "date" not in form_vars and "end_date" not in form_vars:
                    form.errors["status"] = error
                return

        if "status" in form_vars:
            # CURRENT only allowed when org verification valid
            if status == "CURRENT" and \
               not TestProvider(organisation_id).verification.accepted:
                form.errors["status"] = T("Organisation not verified")

            # CURRENT/SUSPENDED only allowed before end date
            today = current.request.utcnow.date()
            if end and end <= today and status in active_statuses:
                form.errors["status"] = T("Invalid status past end date")
                return

            # SUSPENDED requires a reason
            reason = form_vars.get("status_reason") or ""
            if status == "SUSPENDED" and "status_reason" in form_vars and len(reason.strip()) < 3:
                form.errors["status_reason"] = T("Reason required for suspended-status")
                return

    #--------------------------------------------------------------------------
    @staticmethod
    def commission_onaccept(form):
        """
            Onaccept of commission form
                - set status EXPIRED when end date is past
                - set status SUSPENDED when provider not verified
                + when status changed:
                    - set status date and prev_status
                    - trigger facility approval updates
                    - notify commission change
        """

        record_id = get_form_record_id(form)
        if not record_id:
            return

        db = current.db
        s3db = current.s3db

        table = s3db.org_commission
        record = db(table.id == record_id).select(table.id,
                                                  table.organisation_id,
                                                  table.end_date,
                                                  table.prev_status,
                                                  table.status,
                                                  table.status_reason,
                                                  limitby = (0, 1),
                                                  ).first()
        if not record:
            return

        provider = TestProvider(record.organisation_id)
        today = current.request.utcnow.date()

        update = {}
        if provider.verification.accepted:
            if record.end_date < today:
                update["status"] = "EXPIRED"
                update["status_reason"] = None
        elif record.status == "CURRENT":
            update["status"] = "SUSPENDED"
            update["status_reason"] = "N/V"
        elif record.status in ("CURRENT", "REVOKED", "EXPIRED"):
            update["status_reason"] = None

        new_status = update.get("status") or record.status
        status_change = new_status != record.prev_status

        if update:
            if status_change:
                update["status_date"] = today
                update["prev_status"] = new_status
            record.update_record(**update)

        if status_change:
            # Deactivate/reactivate all test stations
            public = "Y" if new_status == "CURRENT" else "N"
            TestStation.update_all(record.organisation_id,
                                   public = public,
                                   reason = "COMMISSION",
                                   )
            # Notify the provider
            provider.notify_commission_change(status = new_status,
                                              reason = record.status_reason,
                                              )

# =============================================================================
class TestStationModel(DataModel):
    """
        Data model for test station approval and approval history
    """

    names = ("org_site_approval",
             "org_site_approval_status",
             )

    def model(self):

        T = current.T

        organisation_id = self.org_organisation_id
        site_id = self.org_site_id

        define_table = self.define_table
        configure = self.configure

        crud_strings = current.response.s3.crud_strings

        # ---------------------------------------------------------------------
        # Workflow options
        #
        status_opts = (("REVISE", T("Completion/Adjustment Required")),
                       ("READY", T("Ready for Review")),
                       ("REVIEW", T("Review Pending")),
                       ("APPROVED", T("Approved##actionable")),
                       )
        status_represent = workflow_tag_represent(dict(status_opts))

        review_opts = (("REVISE", T("Completion/Adjustment Required")),
                       ("REVIEW", T("Review Pending")),
                       ("APPROVED", T("Approved##actionable")),
                       )
        review_represent = workflow_tag_represent(dict(review_opts))

        binary_opts = (("N", T("No")), ("Y", T("Yes")))
        binary_represent = workflow_tag_represent(dict(binary_opts, none="N"))

        public_reasons = {"NEW": T("New registration"),
                          "COMMISSION": T("Organisation not currently commissioned"),
                          "REVISE": T("Documentation incomplete"),
                          "REVIEW": T("Review pending"),
                          "OVERRIDE": T("De-listed manually"),
                          }

        # ---------------------------------------------------------------------
        # Current approval details
        #
        tablename = "org_site_approval"
        define_table(tablename,
                     organisation_id(),
                     site_id(),
                     # Hidden data hash to detect relevant changes
                     Field("dhash",
                           readable = False,
                           writable = False,
                           ),
                     # Workflow status
                     Field("status",
                           label = T("Processing Status"),
                           default = "REVISE",
                           requires = IS_IN_SET(status_opts,
                                                zero = None,
                                                sort = False,
                                                ),
                           represent = status_represent,
                           readable = True,
                           writable = False, # restricted to applicants
                           ),
                     # MPAV qualification
                     Field("mpav",
                           label = T("MPAV Qualification"),
                           default = "REVISE",
                           requires = IS_IN_SET(review_opts,
                                                zero = None,
                                                sort = False,
                                                ),
                           represent = review_represent,
                           readable = True,
                           writable = False, # restricted to approvers
                           ),
                     # Hygiene concept
                     Field("hygiene",
                           label = T("Hygiene Plan"),
                           default = "REVISE",
                           requires = IS_IN_SET(review_opts,
                                                zero = None,
                                                sort = False,
                                                ),
                           represent = review_represent,
                           readable = True,
                           writable = False, # restricted to approvers
                           ),
                     # Facility layout
                     Field("layout",
                           label = T("Facility Layout Plan"),
                           default = "REVISE",
                           requires = IS_IN_SET(review_opts,
                                                zero = None,
                                                sort = False,
                                                ),
                           represent = review_represent,
                           readable = True,
                           writable = False, # restricted to approvers
                           ),
                     # Listed in public registry
                     Field("public",
                           label = T("In Public Registry"),
                           default = "N",
                           requires = IS_IN_SET(binary_opts, zero=None, sort=False),
                           represent = binary_represent,
                           readable = True,
                           writable = False, # restricted to approvers
                           ),
                     Field("public_reason",
                           label = T("Reason for unlisting"),
                           default = "REVISE",
                           represent = represent_option(public_reasons),
                           readable = True,
                           writable = False,
                           ),
                     Field("advice", "text",
                           label = T("Advice"),
                           comment = DIV(_class="tooltip",
                                         _title="%s|%s" % (T("Advice"),
                                                           T("Instructions/advice for the test station how to proceed with regard to authorization"),
                                                           ),
                                         ),
                           represent = s3_text_represent,
                           widget = s3_comments_widget,
                           ),
                     *s3_meta_fields())

        # Table configuration
        configure(tablename,
                  onaccept = self.site_approval_onaccept,
                  )

        # ---------------------------------------------------------------------
        # Historic approval statuses
        # - written onaccept of org_site_approval when values change
        #
        tablename = "org_site_approval_status"
        define_table(tablename,
                     site_id(),
                     s3_datetime("timestmp", writable=False),
                     Field("status",
                           label = T("Processing Status"),
                           represent = status_represent,
                           writable = False,
                           ),
                     Field("mpav",
                           label = T("MPAV Qualification"),
                           represent = review_represent,
                           writable = False,
                           ),
                     Field("hygiene",
                           label = T("Hygiene Plan"),
                           represent = review_represent,
                           writable = False,
                           ),
                     Field("layout",
                           label = T("Facility Layout Plan"),
                           represent = review_represent,
                           writable = False,
                           ),
                     Field("public",
                           label = T("In Public Registry"),
                           represent = binary_represent,
                           writable = False,
                           ),
                     Field("public_reason",
                           label = T("Reason for unlisting"),
                           represent = represent_option(public_reasons),
                           readable = True,
                           writable = False,
                           ),
                     Field("advice", "text",
                           label = T("Advice"),
                           represent = s3_text_represent,
                           writable = False,
                           ),
                     *s3_meta_fields())

        # List fields
        list_fields = ["timestmp",
                       "status",
                       "mpav",
                       "hygiene",
                       "layout",
                       "public",
                       "advice",
                       ]

        # Table configuration
        configure(tablename,
                  insertable = False,
                  editable = False,
                  deletable = False,
                  list_fields = list_fields,
                  orderby = "%s.timestmp desc" % tablename,
                  )

        # CRUD strings
        crud_strings[tablename] = Storage(
            title_display = T("Approval Status"),
            title_list = T("Approval Status History"),
            label_list_button = T("See Approval History"),
            msg_list_empty = T("No Approval Statuses currently registered"),
            )

        # ---------------------------------------------------------------------
        # Return additional names to response.s3
        #
        return None

    # -------------------------------------------------------------------------
    @staticmethod
    def site_approval_status_fields():
        """
            The fields that constitute the current approval status
        """

        return ("status",
                "mpav",
                "hygiene",
                "layout",
                "public",
                "public_reason",
                "advice",
                )

    # -------------------------------------------------------------------------
    @classmethod
    def site_approval_onaccept(cls, form):
        """
            Onaccept of site approval:
                - set public_reason if missing
                - set organisation_id
        """

        db = current.db
        s3db = current.s3db

        # Get record ID
        record_id = get_form_record_id(form)
        if not record_id:
            return

        status_fields = cls.site_approval_status_fields()

        # Re-read record
        atable = s3db.org_site_approval
        query = (atable.id == record_id) & \
                (atable.deleted == False)
        fields = [atable[fn] for fn in (("id", "organisation_id", "site_id") + status_fields)]
        record = db(query).select(*fields, limitby=(0, 1)).first()
        if not record:
            return

        update = {}

        # Set/remove public-reason as required
        if record.public == "N" and not record.public_reason:
            update["public_reason"] = "OVERRIDE"
        if record.public == "Y":
            update["public_reason"] = None

        # Set organisation_id if missing
        ts = TestStation(record.site_id)
        if record.organisation_id != ts.organisation_id:
            update["organisation_id"] = ts.organisation_id

        if update:
            record.update_record(**update)

# =============================================================================
class TestProvider:
    """
        Service functions for the provider verification/commissioning workflow
    """

    def __init__(self, organisation_id):
        """
            Args:
                organisation_id: the org_organisation record ID
        """

        self.organisation_id = organisation_id

        self._record = None
        self._verification = None
        self._commission = None

        self._types = None

    # -------------------------------------------------------------------------
    # Instance properties
    # -------------------------------------------------------------------------
    @property
    def record(self):
        """
            The current organisation record

            Returns:
                - org_organisation Row
        """

        record = self._record
        if not record:
            table = current.s3db.org_organisation
            query = (table.id == self.organisation_id) & \
                    (table.deleted == False)
            record = current.db(query).select(table.id,
                                              table.name,
                                              limitby = (0, 1),
                                              ).first()
            self._record = record

        return record

    # -------------------------------------------------------------------------
    @property
    def verification(self):
        """
            The current verification record for this organisation

            Returns:
                - org_verification Row
        """

        verification = self._verification

        if not verification:
            verification = self.lookup_verification()
            if not verification:
                verification = self.add_verification_defaults()

            self._verification = verification

        return verification

    # -------------------------------------------------------------------------
    @property
    def current_commission(self):
        """
            The current commission record, i.e.
                - with status CURRENT and valid for the current date

            Returns:
                - org_commission Row
        """

        commission = self._commission
        if not commission:
            table = current.s3db.org_commission
            today = current.request.utcnow.date()

            query = (table.organisation_id == self.organisation_id) & \
                    (table.status == "CURRENT") & \
                    ((table.date == None) | (table.date <= today)) & \
                    ((table.end_date == None) | (table.end_date >= today)) & \
                    (table.deleted == False)
            row = current.db(query).select(table.id,
                                           table.date,
                                           table.end_date,
                                           table.status,
                                           limitby = (0, 1),
                                           orderby = ~table.date,
                                           ).first()
            if row:
                commission = self._commission = row

        return commission

    # -------------------------------------------------------------------------
    @property
    def types(self):
        """
            The types and corresponding type tags for the organisation type
            of this provider

            Returns:
                dict {type_id: {tag: value}} with all current types and tags
        """

        types = self._types
        if types is None:

            types = {}

            db = current.db
            s3db = current.s3db

            ltable = s3db.org_organisation_organisation_type
            ttable = s3db.org_organisation_type_tag

            left = ttable.on((ttable.organisation_type_id == ltable.organisation_type_id) & \
                             (ttable.deleted == False))
            query = (ltable.organisation_id == self.organisation_id) & \
                    (ltable.deleted == False)
            rows = db(query).select(ltable.organisation_type_id,
                                    ttable.tag,
                                    ttable.value,
                                    left=left,
                                    )
            for row in rows:
                tag = row[ttable]
                type_id = row[ltable].organisation_type_id
                if type_id not in types:
                    tags = types[type_id] = {}
                else:
                    tags = types[type_id]
                if tag.tag:
                    tags[tag.tag] = tag.value

            self._types = types

        return types

    # -------------------------------------------------------------------------
    @property
    def commercial(self):
        """
            Whether this is a commercial provider

            Returns:
                bool
        """

        types = self.types
        return any(types[t].get("Commercial") == "Y" for t in types)

    # -------------------------------------------------------------------------
    @property
    def verifreq(self):
        """
            Whether organisation type verification is required for this provider

            Returns:
                bool
        """

        types = self.types
        return any(types[t].get("VERIFREQ") == "Y" for t in types)

    # -------------------------------------------------------------------------
    @property
    def minforeq(self):
        """
            Whether manager information is required for this provider

            Returns:
                bool
        """

        types = self.types
        return any(types[t].get("MINFOREQ") == "Y" for t in types)

    # -------------------------------------------------------------------------
    # Instance methods
    # -------------------------------------------------------------------------
    def lookup_verification(self, query=None):
        """
            Looks up the current verification status of this provider

            Args:
                query: the query to use for the lookup (optional)

            Returns:
                org_verification Row
        """

        table = current.s3db.org_verification

        if query is None:
            query = (table.organisation_id == self.organisation_id) & \
                    (table.deleted == False)

        verification = current.db(query).select(table.id,
                                                table.dhash,
                                                table.mgrinfo,
                                                table.orgtype,
                                                table.accepted,
                                                limitby = (0, 1),
                                                ).first()
        return verification

    # -------------------------------------------------------------------------
    def verification_defaults(self):
        """
            Gets defaults for the verification record for this provider
                - defaults depend on organisation type

            Returns:
                dict {fieldname: value}
        """

        if self.types:
            orgtype = "N/V" if self.verifreq else "ACCEPT"
        else:
            orgtype = "N/A"
        mgrinfo = self.check_mgrinfo() if self.minforeq else "ACCEPT"

        accepted = orgtype in ("ACCEPT", "VERIFIED") and \
                   mgrinfo in ("ACCEPT", "COMPLETE")

        return {"orgtype": orgtype,
                "mgrinfo": mgrinfo,
                "accepted": accepted,
                }

    # -----------------------------------------------------------------------------
    def add_default_tags(self):
        """
            Adds default tags for this provider (DELIVERY and OrgID)

            Notes:
                - to be called create-onaccept of organisations
        """

        db = current.db
        s3db = current.s3db

        # Look up current tags
        otable = s3db.org_organisation
        ttable = s3db.org_organisation_tag
        dttable = ttable.with_alias("delivery")
        ittable = ttable.with_alias("orgid")

        left = [dttable.on((dttable.organisation_id == otable.id) & \
                           (dttable.tag == "DELIVERY") & \
                           (dttable.deleted == False)),
                ittable.on((ittable.organisation_id == otable.id) & \
                           (ittable.tag == "OrgID") & \
                           (ittable.deleted == False)),
                ]
        query = (otable.id == self.organisation_id)
        row = db(query).select(otable.id,
                               otable.uuid,
                               dttable.id,
                               ittable.id,
                               left = left,
                               limitby = (0, 1),
                               ).first()
        if row:
            # Add default tags as required
            org = row.org_organisation

            # Add DELIVERY-tag
            dtag = row.delivery
            if not dtag.id:
                ttable.insert(organisation_id = org.id,
                              tag = "DELIVERY",
                              value = "DIRECT",
                              )
            # Add OrgID-tag
            itag = row.orgid
            if not itag.id:
                try:
                    uid = int(org.uuid[9:14], 16)
                except (TypeError, ValueError):
                    import uuid
                    uid = int(uuid.uuid4().urn[9:14], 16)
                value = "%06d%04d" % (uid, org.id)
                ttable.insert(organisation_id = org.id,
                              tag = "OrgID",
                              value = value,
                              )

    # -------------------------------------------------------------------------
    def add_verification_defaults(self):
        """
            Adds the default verification status for this provider

            Returns:
                org_verification Row

            Notes:
                - should be called during organisation post-process, not
                  onaccept (because type links are written only after onaccept)
                - required both during registration approval and manual
                  creation of organisation
        """

        data = self.verification_defaults()
        data["organisation_id"] = self.organisation_id

        table = current.s3db.org_verification
        record_id = table.insert(**data)

        return self.lookup_verification(table.id == record_id)

    # -------------------------------------------------------------------------
    def vhash(self):
        """
            Produces a data hash for this provider, to be stored in the
            verification record for detection of verification-relevant
            data changes

            Returns:
                tuple (update, vhash), where
                - update is a dict with updates for the verification record
                - hash is the (updated) data hash
        """

        # Compute the vhash
        types = "|".join(str(x) for x in sorted(self.types))
        vhash = get_dhash([types])

        # Check the current hash to detect relevant changes
        if vhash != self.verification.dhash:
            # Data have changed
            # => reset verification to type-specific defaults
            update = self.verification_defaults()
            update["dhash"] = vhash
        else:
            update = None

        return update, vhash

    # -------------------------------------------------------------------------
    @staticmethod
    def reset_all(tags, value="N/A"):
        """
            Sets all given workflow tags to initial status

            Args:
                tags: the tag Rows
                value: the initial value
        """

        for tag in tags:
            tag.update_record(value=value)

    # -------------------------------------------------------------------------
    def check_mgrinfo(self):
        """
            Checks whether the manager documentation for this provider is
            complete and verified/accepted

            Returns:
                status N/A|REVISE|COMPLETE

            Notes:
                - does not evaluate whether manager info is required
        """

        from ..config import TESTSTATIONS
        from ..helpers import is_org_group

        organisation_id = self.organisation_id

        # Check if the organisation belongs to the TESTSTATIONS group
        if not is_org_group(organisation_id, TESTSTATIONS):
            return None

        db = current.db
        s3db = current.s3db

        # Look up test station managers, and related data/tags
        ptable = s3db.pr_person
        htable = s3db.hrm_human_resource

        httable = s3db.hrm_human_resource_tag
        reg_tag = httable.with_alias("reg_tag")
        crc_tag = httable.with_alias("crc_tag")
        scp_tag = httable.with_alias("scp_tag")
        dsh_tag = httable.with_alias("dsh_tag")

        join = ptable.on(ptable.id == htable.person_id)
        left = [reg_tag.on((reg_tag.human_resource_id == htable.id) & \
                           (reg_tag.tag == "REGFORM") & \
                           (reg_tag.deleted == False)),
                crc_tag.on((crc_tag.human_resource_id == htable.id) & \
                           (crc_tag.tag == "CRC") & \
                           (crc_tag.deleted == False)),
                scp_tag.on((scp_tag.human_resource_id == htable.id) & \
                           (scp_tag.tag == "SCP") & \
                           (scp_tag.deleted == False)),
                dsh_tag.on((dsh_tag.human_resource_id == htable.id) & \
                           (dsh_tag.tag == "DHASH") & \
                           (dsh_tag.deleted == False)),
                ]

        query = (htable.organisation_id == organisation_id) & \
                (htable.org_contact == True) & \
                (htable.status == 1) & \
                (htable.deleted == False)

        rows = db(query).select(htable.id,
                                ptable.pe_id,
                                ptable.first_name,
                                ptable.last_name,
                                ptable.date_of_birth,
                                dsh_tag.id,
                                dsh_tag.value,
                                reg_tag.id,
                                reg_tag.value,
                                crc_tag.id,
                                crc_tag.value,
                                scp_tag.id,
                                scp_tag.value,
                                join = join,
                                left = left,
                                )
        if not rows:
            # No managers selected
            status = "N/A"
        else:
            # Managers selected => check data/documentation
            status = "REVISE"
            ctable = s3db.pr_contact

            reset_all = self.reset_all

            for row in rows:

                person = row.pr_person
                dob = person.date_of_birth
                vhash = get_dhash(person.first_name,
                                  person.last_name,
                                  dob.isoformat() if dob else None,
                                  )
                doc_tags = [row[t._tablename] for t in (reg_tag, crc_tag, scp_tag)]

                # Do we have a verification hash (after previous approval)?
                dhash = row.dsh_tag
                verified = bool(dhash.id)
                accepted = True

                # Check completeness/integrity of data

                # Must have DoB
                if accepted and not dob:
                    # No documentation can be approved without DoB
                    reset_all(doc_tags)
                    accepted = False

                # Must have at least one contact detail of the email/phone type
                if accepted:
                    query = (ctable.pe_id == row.pr_person.pe_id) & \
                            (ctable.contact_method in ("SMS", "HOME_PHONE", "WORK_PHONE", "EMAIL")) & \
                            (ctable.value != None) & \
                            (ctable.deleted == False)
                    contact = db(query).select(ctable.id, limitby=(0, 1)).first()
                    if not contact:
                        accepted = False

                # Do the data (still) match the verification hash?
                if accepted and verified:
                    if dhash.value != vhash:
                        if current.auth.s3_has_role("ORG_GROUP_ADMIN"):
                            # Data changed by OrgGroupAdmin => update hash
                            # (authorized change has no influence on approval)
                            dhash.update_record(value=vhash)
                        else:
                            # Data changed by someone else => previous
                            # approval of documentation no longer valid
                            reset_all(doc_tags)
                            accepted = False

                # Check approval status for documentation
                if accepted and all(tag.value == "APPROVED" for tag in doc_tags):
                    if not verified:
                        # Set the verification hash
                        dsh_tag.insert(human_resource_id = row[htable.id],
                                       tag = "DHASH",
                                       value = vhash,
                                       )
                    # If at least one record is acceptable, the manager-data
                    # status of the organisation can be set as complete
                    status = "COMPLETE"
                else:
                    # Remove the verification hash, if any (unapproved records
                    # do not need to be integrity-checked)
                    if verified:
                        dhash.delete_record()

        return status

    # -------------------------------------------------------------------------
    def update_verification(self):
        """
            Updates the verification status of this provider, to be called
            whenever relevant details change:
                - organisation form post-process
                - staff record, person record, contact details
                - TODO org type tags regarding verification requirements
        """

        verification = self.verification

        update = self.vhash()[0]
        if update:
            accepted = verification.accepted
        else:
            update = {}

            orgtype = self.verification.orgtype

            mgrinfo = self.check_mgrinfo() if self.minforeq else "ACCEPT"
            if mgrinfo != verification.mgrinfo:
                update["mgrinfo"] = mgrinfo

            accepted = orgtype in ("ACCEPT", "VERIFIED") and \
                       mgrinfo in ("ACCEPT", "COMPLETE")
            if accepted != verification.accepted:
                update["accepted"] = accepted

        if update:
            verification.update_record(**update)

        if accepted:
            self.reinstate_commission("N/V")
        else:
            self.suspend_commission("N/V")

    # -------------------------------------------------------------------------
    def suspend_commission(self, reason):
        """
            Suspends all current commissions of this provider

            Args:
                reason: the reason code for suspension (required)
        """

        if not reason:
            raise RuntimeError("reason required")

        db = current.db
        s3db = current.s3db

        table = s3db.org_commission
        query = (table.organisation_id == self.organisation_id) & \
                (table.status == "CURRENT") & \
                (table.deleted == False)
        updated = db(query).update(status = "SUSPENDED",
                                   status_date = current.request.utcnow.date(),
                                   status_reason = reason,
                                   modified_by = table.modified_by,
                                   modified_on = table.modified_on,
                                   )
        if updated:
            self.notify_commission_change(status = "SUSPENDED",
                                          reason = reason,
                                          )

        TestStation.update_all(self.organisation_id,
                               public = "N",
                               reason = "COMMISSION",
                               )

    # -------------------------------------------------------------------------
    def reinstate_commission(self, reason):
        """
            Reinstates commissions of this provider that have previously
            been suspended for the given reason

            Args:
                reason: the reason code (required)
        """

        if not reason:
            raise RuntimeError("reason required")

        db = current.db
        s3db = current.s3db

        table = s3db.org_commission
        query = (table.organisation_id == self.organisation_id) & \
                (table.status == "SUSPENDED") & \
                (table.status_reason == reason) & \
                (table.deleted == False)
        updated = db(query).update(status = "CURRENT",
                                   status_date = current.request.utcnow.date(),
                                   status_reason = None,
                                   modified_by = table.modified_by,
                                   modified_on = table.modified_on,
                                   )
        if updated:
            self.notify_commission_change(status = "CURRENT",
                                          reason = reason,
                                          )

        TestStation.update_all(self.organisation_id,
                               public = "Y",
                               reason = "COMMISSION",
                               )

    # -------------------------------------------------------------------------
    def notify_commission_change(self, status=None, reason=None):
        # TODO implement

        import sys
        sys.stderr.write("Notifying provider %s about commission change\n" % self.organisation_id)
        sys.stderr.write("status=%s, reason=%s\n" % (status, reason))

    # -------------------------------------------------------------------------
    # Configuration helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def add_components():
        """
            Adds org_organisation components for verification/commission
        """

        current.s3db.add_components("org_organisation",
                                    org_verification = {"joinby": "organisation_id",
                                                        "multiple": False,
                                                        },
                                    org_commission = "organisation_id",
                                    )

    # -------------------------------------------------------------------------
    @classmethod
    def configure_verification(cls, resource, role="applicant", record_id=None):
        """
            Configures the verification subform for CRUD

            Args:
                resource: the org_organisation resource
                role: applicant|approver
                record_id: the org_organisation record ID
        """

        component = resource.components.get("verification")
        if not component:
            return
        table = component.table

        if record_id:
            is_approver = role == "approver"

            provider = cls(record_id)

            field = table.orgtype
            if provider.verifreq:
                # Configure selectable options
                selectable = {"N/V", "VERIFIED"}
                current_value = provider.verification.orgtype
                if current_value not in selectable:
                    field.writable = False
                else:
                    options = TestProviderModel.orgtype_status_opts()
                    options = [o for o in options if o[0] in selectable]
                    field.requires = IS_IN_SET(options,
                                               sort = False,
                                               zero = None,
                                               )
                    field.writable = is_approver
            else:
                field.readable = False

            field = table.mgrinfo # not manually writable
            if not provider.minforeq:
                field.readable = False

        else:
            for fn in ("orgtype", "mgrinfo"):
                field = table[fn]
                field.readable = field.writable = False

    # -------------------------------------------------------------------------
    @staticmethod
    def configure_commission(resource,
                             role = "applicant",
                             record_id = None,
                             commission_id = None,
                             ):
        """
            Configures the commission resource and form for CRUD

            Args:
                resource: the org_commission resource
                role: applicant|provider
                record_id: the org_organisation record ID
                commission_id: the org_commission record ID
        """

        table = resource.table

        if role == "approver":
            if commission_id:
                # Get the record
                query = (table.id == commission_id) & \
                        (table.deleted == False)
                commission = current.db(query).select(table.status,
                                                      limitby = (0, 1),
                                                      ).first()
            else:
                commission = None

            if record_id:
                accepted = TestProvider(record_id).verification.accepted
            else:
                accepted = False

            # Status only writable if CURRENT|SUSPENDED
            if commission:
                if commission.status in ("CURRENT", "SUSPENDED"):
                    editable = True
                    field = table.status
                    field.writable = True
                    selectable = True if accepted else ("SUSPENDED", "REVOKED")
                    options = TestProviderModel.commission_status_opts(selectable=selectable)
                    field.requires = IS_IN_SET(options, sort=False, zero=None)
                elif commission.status in ("REVOKED", "EXPIRED"):
                    editable = False
            else:
                editable = True

            resource.configure(insertable = accepted,
                               editable = editable,
                               )
        else:
            # Render read-only
            for fn in table.fields:
                field = table[fn]
                field.readable = field.writable = False

# =============================================================================
class TestStation:
    """
        Service functions for the test station approval/publication workflow
    """

    def __init__(self, site_id=None, facility_id=None):
        """
            Args:
                site_id: the site ID
                facility_id: the facility record ID, alternatively

            Notes:
                - facility_id will be ignored when site_id is given
        """

        self._approval = None

        if site_id:
            self._site_id = site_id
            self._facility_id = None
        else:
            self._site_id = None
            self._facility_id = facility_id

        self._record = None

    # -------------------------------------------------------------------------
    # Instance properties
    # -------------------------------------------------------------------------
    @property
    def site_id(self):
        """
            The site ID of this test station

            Returns:
                - site ID
        """

        site_id = self._site_id
        if not site_id:
            record = self.record
            site_id = record.site_id if site_id else None

        return site_id

    # -------------------------------------------------------------------------
    @property
    def facility_id(self):
        """
            The facility record ID of this test station

            Returns:
                - the record ID
        """

        facility_id = self._facility_id
        if not facility_id:
            record = self.record
            facility_id = record.id if record else None

        return facility_id

    # -------------------------------------------------------------------------
    @property
    def organisation_id(self):
        """
            The record ID of the organisation this test station belongs to

            Returns:
                - the organisation record ID
        """

        record = self.record

        return record.organisation_id if record else None

    # -------------------------------------------------------------------------
    @property
    def record(self):
        """
            The current org_facility record

            Returns:
                org_facility Row
        """

        record = self._record
        if not record:
            table = current.s3db.org_facility
            site_id, facility_id = self._site_id, self._facility_id
            if site_id:
                query = (table.site_id == site_id)
            else:
                query = (table.id == facility_id)
            query &= (table.deleted == False)
            record = current.db(query).select(table.id,
                                              table.uuid,
                                              table.code,
                                              table.name,
                                              table.site_id,
                                              table.organisation_id,
                                              table.location_id,
                                              limitby = (0, 1),
                                              ).first()
            if record:
                self._record = record
                self._facility_id = record.id
                self._site_id = record.site_id

        return record

    # -------------------------------------------------------------------------
    @property
    def approval(self):
        """
            The current approval status record

            Returns:
                - org_site_approval Row
        """

        approval = self._approval
        if not approval:
            approval = self._approval = self.lookup_approval()
            if not approval:
                # Create approval status record with defaults
                approval = self.add_approval_defaults()

        return approval

    # -------------------------------------------------------------------------
    # Instance methods
    # -------------------------------------------------------------------------
    def lookup_approval(self, query=None):
        """
            Looks up the current approval status of this test station

            Args:
                query: the query to use for the lookup (override)

            Returns:
                org_site_approval Row
        """

        table = current.s3db.org_site_approval

        if query is None:
            query = (table.site_id == self.site_id) & \
                    (table.deleted == False)

        return current.db(query).select(table.id,
                                        table.dhash,
                                        table.status,
                                        table.mpav,
                                        table.hygiene,
                                        table.layout,
                                        table.public,
                                        table.public_reason,
                                        table.advice,
                                        limitby = (0, 1),
                                        ).first()

    # -------------------------------------------------------------------------
    def add_approval_defaults(self):
        """
            Adds the default approval status for this test station

            Returns:
                org_site_approval Row
        """

        table = current.s3db.org_site_approval

        record_id = table.insert(site_id = self.site_id,
                                 organisation_id = self.organisation_id,
                                 public = "N",
                                 public_reason = "NEW",
                                 )

        self._approval = self.lookup_approval(table.id == record_id)

        return self._approval

    # -------------------------------------------------------------------------
    def add_facility_code(self):
        """
            Adds a facility code (Test Station ID) for this test station

            returns:
                the facility code
        """

        facility = self.record

        if not facility or facility.code:
            return None

        try:
            uid = int(facility.uuid[9:14], 16) % 1000000
        except (TypeError, ValueError):
            import uuid
            uid = int(uuid.uuid4().urn[9:14], 16) % 1000000

        # Generate code
        import random
        suffix = "".join(random.choice("ABCFGHKLNPRSTWX12456789") for _ in range(3))
        code = "%06d-%s" % (uid, suffix)

        facility.update_record(code=code)

        return code

    # -------------------------------------------------------------------------
    def vhash(self):
        """
            Computes and checks the verification hash for facility details

            Returns:
                tuple (update, vhash), where
                - update is a dict with workflow tag updates
                - vhash is the computed verification hash

            Notes:
                - the verification hash encodes certain facility details, so
                  if those details are changed after approval, then the hash
                  becomes invalid and any previous approval is overturned
                  (=reduced to review-status)
                - if the user is OrgGroupAdmin or Admin, the approval workflow
                  status is kept as-is (i.e. Admins can change details without
                  that impacting the current workflow status)
        """

        db = current.db
        s3db = current.s3db

        approval = self.approval

        # Extract the location, and compute the hash
        ltable = s3db.gis_location
        query = (ltable.id == self.record.location_id) & \
                (ltable.deleted == False)
        location = db(query).select(ltable.id,
                                    ltable.parent,
                                    ltable.addr_street,
                                    ltable.addr_postcode,
                                    limitby = (0, 1),
                                    ).first()
        if location:
            vhash = get_dhash(location.id,
                              location.parent,
                              location.addr_street,
                              location.addr_postcode,
                              )
        else:
            vhash = get_dhash(None, None, None, None)

        # Check against the current dhash
        dhash = approval.dhash
        if approval.status == "APPROVED" and dhash and dhash != vhash and \
           not current.auth.s3_has_role("ORG_GROUP_ADMIN"):

            # Relevant data have changed

            # Remove from public list, pending revision/review
            update = {"public": "N"}

            # Status update:
            # - details that were previously approved, must be reviewed
            # - overall status becomes review, unless there are details under revision
            status = "REVIEW"
            for t in ("mpav", "hygiene", "layout"):
                value = approval[t]
                if value == "APPROVED":
                    update[t] = "REVIEW"
                elif value == "REVISE":
                    status = "REVISE"
            update["status"] = status

        else:
            update = None

        return update, vhash

    # -----------------------------------------------------------------------------
    def approval_workflow(self):
        """
            Determines which site approval tags to update after status change
            by OrgGroupAdmin

            Returns:
                tuple (update, notify)
                    update: dict {tag: value} for update
                    notify: boolean, whether to notify the OrgAdmin
        """

        tags = self.approval
        update, notify = {}, False

        SITE_REVIEW = ("mpav", "hygiene", "layout")
        all_tags = lambda v: all(tags[k] == v for k in SITE_REVIEW)
        any_tags = lambda v: any(tags[k] == v for k in SITE_REVIEW)

        status = tags.status
        if status == "REVISE":
            if all_tags("APPROVED"):
                update["public"] = "Y"
                update["status"] = "APPROVED"
                notify = True
            elif any_tags("REVIEW"):
                update["public"] = "N"
                update["status"] = "REVIEW"
            else:
                update["public"] = "N"
                # Keep status REVISE

        elif status == "READY":
            update["public"] = "N"
            if all_tags("APPROVED"):
                for k in SITE_REVIEW:
                    update[k] = "REVIEW"
            else:
                for k in SITE_REVIEW:
                    if tags[k] == "REVISE":
                        update[k] = "REVIEW"
            update["status"] = "REVIEW"

        elif status == "REVIEW":
            if all_tags("APPROVED"):
                update["public"] = "Y"
                update["status"] = "APPROVED"
                notify = True
            elif any_tags("REVIEW"):
                update["public"] = "N"
                # Keep status REVIEW
            elif any_tags("REVISE"):
                update["public"] = "N"
                update["status"] = "REVISE"
                notify = True

        elif status == "APPROVED":
            if any_tags("REVIEW"):
                update["public"] = "N"
                update["status"] = "REVIEW"
            elif any_tags("REVISE"):
                update["public"] = "N"
                update["status"] = "REVISE"
                notify = True

        return update, notify

    # -------------------------------------------------------------------------
    def update_approval(self, commissioned=None):
        """
            Updates facility approval workflow tags after status change by
            OrgGroupAdmin, and notify the OrgAdmin of the site when needed

            Args:
                commissioned: whether the organisation has a current
                              commission (will be looked up if omitted)
        """

        approval = self.approval

        # Check if organisation has a current commission
        if commissioned is None:
            organisation_id = self.record.organisation_id
            if organisation_id:
                commissioned = bool(TestProvider(organisation_id).current_commission)

        # Verify record integrity and compute the verification hash
        update, vhash = self.vhash()

        notify = False
        if not update:
            # Integrity check okay => proceed to workflow status
            update, notify = self.approval_workflow()

        # Set/unset reason for public-status
        update_public = update.get("public")
        if update_public == "N":
            # Determine reason from status
            if update.get("status") == "REVISE":
                update["public_reason"] = "REVISE"
            else:
                update["public_reason"] = "REVIEW"

        elif update_public == "Y" or \
             update_public is None and approval.public == "Y":
            # Check if organisation has a current commission
            if commissioned:
                update["public_reason"] = None
            else:
                update["public"] = "N"
                update["public_reason"] = "COMMISSION"

        # Public=N with non-automatic reason must not be overwritten
        if approval.public == "N" and \
           approval.public_reason not in ("NEW", "COMMISSION", "REVISE", "REVIEW"):
            update.pop("public", None)
            update.pop("public_reason", None)

        # Detect public-status change
        public_changed = "public" in update and update["public"] != approval.public

        # Set data hash when approved (to detect relevant data changes)
        status = update["status"] if "status" in update else approval.status
        update["dhash"] = vhash if status == "APPROVED" else None

        # Update the record
        if update:
            approval.update_record(**update)
            self.update_approval_history()

        T = current.T

        # Screen message on status change
        if public_changed:
            if approval.public == "Y":
                msg = T("Facility added to public registry")
            else:
                msg = T("Facility removed from public registry pending review")
            current.response.information = msg

        # Send Notifications
        if notify:
            msg = self.notify_approval_change()
            if msg:
                current.response.warning = \
                    T("Test station could not be notified: %(error)s") % {"error": msg}
            else:
                current.response.flash = \
                    T("Test station notified")

    # -------------------------------------------------------------------------
    def update_approval_history(self):
        """
            Updates site approval history
                - to be called when approval record is updated
        """

        db = current.db
        s3db = current.s3db

        site_id = self.site_id
        approval = self.approval

        htable = s3db.org_site_approval_status
        status_fields = TestStationModel.site_approval_status_fields()

        # Get last entry of history
        htable = s3db.org_site_approval_status
        query = (htable.site_id == site_id) & \
                (htable.deleted == False)
        fields = [htable[fn] for fn in (("id", "timestmp") + status_fields)]
        prev = db(query).select(*fields,
                                limitby = (0, 1),
                                orderby = ~htable.timestmp,
                                ).first()

        # If status has changed...
        if not prev or any(prev[fn] != approval[fn] for fn in status_fields):

            update = {fn: approval[fn] for fn in status_fields}
            update["site_id"] = site_id

            # Update existing history entry or add a new one
            timestmp = current.request.utcnow
            if prev and prev.timestmp == timestmp:
                prev.update_record(**update)
            else:
                update["timestmp"] = timestmp
                htable.insert(**update)

    # -------------------------------------------------------------------------
    def notify_approval_change(self):
        """
            Notifies the OrgAdmin of a test station about the status of
            the review

            Args:
                site_id: the test facility site ID
                tags: the current workflow tags

            Returns:
                error message on error, else None
        """

        db = current.db
        s3db = current.s3db

        # Lookup the facility
        facility = self.record
        if not facility:
            return "Facility not found"

        # Get the organisation ID
        organisation_id = facility.organisation_id
        if not organisation_id:
            return "Organisation not found"

        # Find the OrgAdmin email addresses
        from ..helpers import get_role_emails
        email = get_role_emails("ORG_ADMIN",
                                organisation_id = organisation_id,
                                )
        if not email:
            return "No Organisation Administrator found"

        # Data for the notification email
        data = {"name": facility.name,
                "url": URL(c = "org",
                           f = "organisation",
                           args = [organisation_id, "facility", facility.id],
                           host = True,
                           ),
                }

        approval = self.approval
        status = approval.status

        if status == "REVISE":
            template = "FacilityReview"

            # Add advice
            advice = approval.advice
            data["advice"] = advice if advice else "-"

            # Add explanations for relevant requirements
            review = (("mpav", "FacilityMPAVRequirements"),
                      ("hygiene", "FacilityHygienePlanRequirements"),
                      ("layout", "FacilityLayoutRequirements"),
                      #("MGRINFO", "TestStationManagerRequirements"),
                      )
            ctable = s3db.cms_post
            ltable = s3db.cms_post_module
            join = ltable.on((ltable.post_id == ctable.id) & \
                             (ltable.module == "org") & \
                             (ltable.resource == "facility") & \
                             (ltable.deleted == False))
            explanations = []
            for tag, requirements in review:
                if approval[tag] == "REVISE":
                    query = (ctable.name == requirements) & \
                            (ctable.deleted == False)
                    row = db(query).select(ctable.body,
                                           join = join,
                                           limitby = (0, 1),
                                           ).first()
                    if row:
                        explanations.append(row.body)
            data["explanations"] = "\n\n".join(explanations) if explanations else "-"

        elif status == "APPROVED":
            template = "FacilityApproved"

        else:
            # No notifications for this status
            return "invalid status"

        # Lookup email address of current user
        from ..notifications import CMSNotifications
        auth = current.auth
        if auth.user:
            cc = CMSNotifications.lookup_contact(auth.user.pe_id)
        else:
            cc = None

        # Send CMS Notification FacilityReview
        return CMSNotifications.send(email,
                                     template,
                                     data,
                                     module = "org",
                                     resource = "facility",
                                     cc = cc,
                                     )

    # -------------------------------------------------------------------------
    # Class methods
    # -------------------------------------------------------------------------
    @classmethod
    def update_all(cls, organisation_id, public=None, reason=None):
        """
            Updates the public-status for all test stations of an
            organisation, to be called when commission status changes

            Args:
                organisation_id: the organisation ID
                public: the new public-status ("Y" or "N")
                reason: the reason(s) for the "N"-status (code|list of codes)

            Notes:
                - can only update those to "Y" which are fully approved
                  and where public_reason matches the given reason(s)
                - reason is required for update to "N"-status
        """

        if public == "N" and not reason:
            raise RuntimeError("reason required")

        db = current.db

        table = current.s3db.org_site_approval
        query = (table.organisation_id == organisation_id) & \
                (table.public != public)

        if public == "Y":
            # Update only to "Y" if fully approved
            for tag in ("status", "mpav", "hygiene", "layout"):
                query &= (table[tag] == "APPROVED")
            # Update only those which match the specified reason
            if isinstance(reason, (tuple, list, set)):
                query &= (table.public_reason.belongs(reason))
            else:
                query &= (table.public_reason == reason)
            update = {"public": "Y", "public_reason": None}
        else:
            update = {"public": "N", "public_reason": reason}
        query &= (table.deleted == False)

        rows = db(query).select(table.site_id)

        # Update the matching facilities
        num_updated = db(query).update(**update)

        # Update approval histories
        for row in rows:
            cls(row.site_id).update_approval_history()

        return num_updated

    # -------------------------------------------------------------------------
    # Configuration helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def add_site_approval():
        """
            Configures approval workflow as component of org_site
                - for embedding in form
        """

        s3db = current.s3db

        s3db.add_components("org_site",
                            org_site_approval = {"name": "approval",
                                                "joinby": "site_id",
                                                "multiple": False,
                                                },
                            org_site_approval_status = "site_id",
                            )

    # -------------------------------------------------------------------------
    @staticmethod
    def configure_site_approval(resource, role="applicant", record_id=None):
        """
            Configures the approval workflow subform

            Args:
                resource: the org_facility resource
                role: the user's role in the workflow (applicant|approver)
                record_id: the facility record ID

            Returns:
                the list of visible workflow tags [(label, selector)]
        """

        visible_tags = []

        component = resource.components.get("approval")
        if not component:
            return None
        ctable = component.table

        # Configure status-field
        #   - applicants can change to READY if current status is REVISE
        #   - read-only for applicants otherwise
        #   - invisible for approvers (default)
        field = ctable.status
        status_tag_opts = dict(field.requires.options())
        review_tags_visible = False
        if role == "applicant" and record_id:

            field.readable = True
            visible_tags.append("approval.status")

            # Get current status to determine selectable values
            db = current.db
            s3db = current.s3db
            ftable = s3db.org_facility
            atable = s3db.org_site_approval
            join = ftable.on((ftable.site_id == atable.site_id) & \
                             (ftable.id == record_id))
            query = (atable.deleted == False)
            row = db(query).select(atable.status, join=join, limitby=(0, 1)).first()

            if row:
                status = row.status
                if status == "REVISE":
                    field.writable = True
                    selectable = [(v, status_tag_opts[v]) for v in ("REVISE", "READY")]
                    field.requires = IS_IN_SET(selectable, zero=None)
                    review_tags_visible = True
                elif status == "REVIEW":
                    field.writable = False
                    review_tags_visible = True

        # Configure review-tags
        #   - read-only for applicants if status REVISE|REVIEW
        #   - invisible for applicants otherwise
        #   - writable for approvers
        review_tags = ("mpav", "hygiene", "layout")
        for fn in review_tags:
            field = ctable[fn]
            field.default = "REVISE"
            if role == "approver":
                field.readable = field.writable = True
            else:
                field.readable = review_tags_visible
                field.writable = False
            if field.readable:
                visible_tags.append("approval.%s" % fn)

        # Configure public-tag
        #   - readable for all
        #   - writable for approvers
        field = ctable.public
        field.writable = role == "approver"

        visible_tags.extend(["approval.public",
                             #"approval.public_reason",
                             "approval.advice",
                             ])

        return visible_tags

# =============================================================================
def get_dhash(*values):
    """
        Produce a data verification hash from the values

        Args:
            values: an (ordered) iterable of values
        Returns:
            the verification hash as string
    """

    import hashlib
    dstr = "#".join([str(v) if v else "***" for v in values])

    return hashlib.sha256(dstr.encode("utf-8")).hexdigest().lower()

# -----------------------------------------------------------------------------
def workflow_tag_represent(options, none=None):
    """
        Color-coded and icon-supported representation of
        organisation/facility approval workflow tags

        Args:
            options: the tag options as dict {value: label}
            none: treat None-values like this option (str)
    """

    icons = {"REVISE": "fa fa-exclamation-triangle",
             "REJECT": "fa fa-exclamation-triangle",
             "REVIEW": "fa fa-hourglass",
             "N/V": "fa fa-hourglass",
             "APPROVED": "fa fa-check",
             "ACCEPT": "fa fa-check",
             "VERIFIED":  "fa fa-check",
             "COMPLETE": "fa fa-check",
             "N/A": "fa fa-minus-circle",
             "N": "fa fa-minus-circle",
             False: "fa fa-minus-circle",
             "Y": "fa fa-check",
             True: "fa fa-check",
             }

    css_classes = {"REVISE": "workflow-red",
                   "REJECT": "workflow-red",
                   "REVIEW": "workflow-amber",
                   "N/V": "workflow-amber",
                   "APPROVED": "workflow-green",
                   "ACCEPT": "workflow-green",
                   "VERIFIED": "workflow-green",
                   "COMPLETE": "workflow-green",
                   "N/A": "workflow-grey",
                   "N": "workflow-red",
                   False: "workflow-red",
                   "Y": "workflow-green",
                   True: "workflow-green",
                   }

    def represent(value, row=None):

        if value is None and none:
            value = none

        label = DIV(_class="approve-workflow")
        color = css_classes.get(value)
        if color:
            label.add_class(color)
        icon = icons.get(value)
        if icon:
            label.append(I(_class=icon))
        label.append(options.get(value, "-"))

        return label

    return represent

# END =========================================================================
