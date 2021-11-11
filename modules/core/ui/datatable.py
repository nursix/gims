"""
    Data Table

    Copyright: 2009-2021 (c) Sahana Software Foundation

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

__all__ = ("S3DataTable",
           )

import re

from gluon import current, URL, \
                  A, DIV, FORM, INPUT, SPAN, TABLE, TBODY, TD, TH, THEAD, TR
from gluon.storage import Storage

from s3dal import Expression, S3DAL

from ..tools import s3_orderby_fields, s3_set_extension, s3_str

# =============================================================================
class S3DataTable:
    """ Class representing a data table """

    # The dataTable id if no explicit value has been provided
    id_counter = 1

    # -------------------------------------------------------------------------
    # Standard API
    # -------------------------------------------------------------------------
    def __init__(self,
                 rfields,
                 data,
                 start = 0,
                 limit = None,
                 filterString = None,
                 orderby = None,
                 empty = False,
                 ):
        """
            Args:
                rfields: A list of S3Resourcefield
                data: A list of Storages the key is of the form table.field
                      The value is the data to be displayed in the dataTable
                start: the first row to return from the data
                limit: the (maximum) number of records to return
                filterString: The string that was used in filtering the records
                orderby: the DAL orderby construct
        """

        self.data = data
        self.rfields = rfields
        self.empty = empty

        colnames = []
        heading = {}

        append = colnames.append
        for rfield in rfields:
            colname = rfield.colname
            heading[colname] = rfield.label
            append(colname)

        self.colnames = colnames
        self.heading = heading

        data_len = len(data)
        if start < 0:
            start = 0
        if start > data_len:
            start = data_len
        if limit == None:
            end = data_len
        else:
            end = start + limit
            if end > data_len:
                end = data_len
        self.start = start
        self.end = end
        self.filterString = filterString

        if orderby:

            # Resolve orderby expression into column names
            orderby_dirs = {}
            orderby_cols = []

            adapter = S3DAL()
            INVERT = adapter.INVERT

            append = orderby_cols.append
            for f in s3_orderby_fields(None, orderby, expr=True):
                if type(f) is Expression:
                    colname = str(f.first)
                    direction = "desc" if f.op == INVERT else "asc"
                else:
                    colname = str(f)
                    direction = "asc"
                orderby_dirs[colname] = direction
                append(colname)
            pos = 0

            # Helper function to resolve a reference's "sortby" into
            # a list of column names
            ftuples = {}
            def resolve_sortby(rfield):
                colname = rfield.colname
                if colname in ftuples:
                    return ftuples[colname]
                ftype = rfield.ftype
                sortby = None
                if ftype[:9] == "reference":
                    field = rfield.field
                    if hasattr(field, "sortby") and field.sortby:
                        sortby = field.sortby
                        if not isinstance(sortby, (tuple, list)):
                            sortby = [sortby]
                        p = "%s.%%s" % ftype[10:].split(".")[0]
                        sortby = [p % fname for fname in sortby]
                ftuples[colname] = sortby
                return sortby

            dt_ordering = [] # order expression for datatable
            append = dt_ordering.append

            # Match orderby-fields against table columns (=rfields)
            seen = set()
            skip = seen.add
            for i, colname in enumerate(orderby_cols):
                if i < pos:
                    # Already consumed by sortby-tuple
                    continue
                direction = orderby_dirs[colname]
                for col_idx, rfield in enumerate(rfields):
                    if col_idx in seen:
                        # Column already in dt_ordering
                        continue
                    sortby = None
                    if rfield.colname == colname:
                        # Match a single orderby-field
                        sortby = (colname,)
                    else:
                        # Match between sortby and the orderby-field tuple
                        # (must appear in same order and sorting direction)
                        sortby = resolve_sortby(rfield)
                        if not sortby or \
                           sortby != orderby_cols[i:i + len(sortby)] or \
                           any(orderby_dirs[c] != direction for c in sortby):
                            sortby = None
                    if sortby:
                        append([col_idx, direction])
                        pos += len(sortby)
                        skip(col_idx)
                        break
        else:
            dt_ordering = [[1, "asc"]]

        self.orderby = dt_ordering

    # -------------------------------------------------------------------------
    def html(self,
             totalrows,
             filteredrows,
             id = None,
             draw = 1,
             **attr
             ):
        """
            Method to render the dataTable into html

            Args:
                totalrows: The total rows in the unfiltered query.
                filteredrows: The total rows in the filtered query.
                id: The id of the table these need to be unique if more
                    than one dataTable is to be rendered on the same page.
                    If this is not passed in then a unique id will be
                    generated. Regardless the id is stored in self.id
                    so it can be easily accessed after rendering.
                draw: An unaltered copy of draw sent from the client used
                      by dataTables as a draw count.
                attr: dictionary of attributes which can be passed in
        """

        flist = self.colnames

        if not id:
            id = "list_%s" % self.id_counter
            self.id_counter += 1
        self.id = id

        attr_get = attr.get

        bulkActions = attr_get("dt_bulk_actions", None)
        bulkCol = attr_get("dt_bulk_col", 0)
        if bulkCol > len(flist):
            bulkCol = len(flist)
        action_col = attr_get("dt_action_col", 0)
        if action_col != 0:
            if action_col == -1 or action_col >= len(flist):
                action_col = len(flist) -1
                attr["dt_action_col"] = action_col
            flist = flist[1:action_col+1] + [flist[0]] + flist[action_col+1:]

        # Get the details for any bulk actions. If we have at least one bulk
        # action then a column will be added, either at the start or in the
        # column identified by dt_bulk_col
        if bulkActions:
            flist.insert(bulkCol, "BULK")
            if bulkCol <= action_col:
                action_col += 1

        pagination = attr_get("dt_pagination", "true") == "true"
        if pagination:
            real_end = self.end
            self.end = self.start + 1
        table = self.table(id, flist, action_col)
        if pagination:
            self.end = real_end
            aadata = self.aadata(totalrows,
                                 filteredrows,
                                 id,
                                 draw,
                                 flist,
                                 action_col=action_col,
                                 stringify=False,
                                 **attr)
            cache = {"cacheLower": self.start,
                     "cacheUpper": self.end if filteredrows > self.end else filteredrows,
                     "cacheLastJson": aadata,
                     }
        else:
            cache = None

        html = self.htmlConfig(table,
                               id,
                               self.orderby,
                               self.rfields,
                               cache,
                               **attr
                               )
        return html

    # -------------------------------------------------------------------------
    @staticmethod
    def i18n():
        """
            Return the i18n strings needed by dataTables
            - called by views/dataTables.html
        """

        T = current.T
        scripts = ['''i18n.sortAscending="%s"''' % T("activate to sort column ascending"),
                   '''i18n.sortDescending="%s"''' % T("activate to sort column descending"),
                   '''i18n.first="%s"''' % T("First"),
                   '''i18n.last="%s"''' % T("Last"),
                   '''i18n.next="%s"''' % T("Next"),
                   '''i18n.previous="%s"''' % T("Previous"),
                   '''i18n.emptyTable="%s"''' % T("No records found"), #T("No data available in table"),
                   '''i18n.info="%s"''' % T("Showing _START_ to _END_ of _TOTAL_ entries"),
                   '''i18n.infoEmpty="%s"''' % T("Showing 0 to 0 of 0 entries"),
                   '''i18n.infoFiltered="%s"''' % T("(filtered from _MAX_ total entries)"),
                   '''i18n.infoThousands="%s"''' % current.deployment_settings.get_L10n_thousands_separator(),
                   '''i18n.lengthMenu="%s"''' % (T("Show %(number)s entries") % {"number": "_MENU_"}),
                   '''i18n.loadingRecords="%s"''' % T("Loading"),
                   '''i18n.processing="%s"''' % T("Processing"),
                   '''i18n.search="%s"''' % T("Search"),
                   '''i18n.zeroRecords="%s"''' % T("No matching records found"),
                   '''i18n.selectAll="%s"''' % T("Select All")
                   ]
        script = "\n".join(scripts)

        return script

    # -------------------------------------------------------------------------
    def json(self,
             totalrows,
             displayrows,
             id,
             draw,
             stringify = True,
             **attr
             ):
        """
            Method to render the data into a json object

            Args:
                totalrows: The total rows in the unfiltered query.
                displayrows: The total rows in the filtered query.
                id: The id of the table for which this ajax call will
                    respond to.
                draw: An unaltered copy of draw sent from the client used
                      by dataTables as a draw count.
                attr: dictionary of attributes which can be passed in

            Keyword Args:
                dt_action_col: The column where the action buttons will be placed
                dt_bulk_actions: list of labels for the bulk actions.
                dt_bulk_col: The column in which the checkboxes will appear,
                             by default it will be the column immediately
                             before the first data item
                dt_group_totals: The number of record in each group.
                                 This will be displayed in parenthesis
                                 after the group title.
        """

        attr_get = attr.get
        flist = self.colnames
        action_col = attr_get("dt_action_col", 0)
        if action_col != 0:
            if action_col == -1 or action_col >= len(flist):
                action_col = len(flist) - 1
            flist = flist[1:action_col+1] + [flist[0]] + flist[action_col+1:]
        # Get the details for any bulk actions. If we have at least one bulk
        # action then a column will be added, either at the start or in the
        # column identified by dt_bulk_col
        bulkActions = attr_get("dt_bulk_actions", None)
        bulkCol = attr_get("dt_bulk_col", 0)
        if bulkActions:
            if bulkCol > len(flist):
                bulkCol = len(flist)
            flist.insert(bulkCol, "BULK")
            if bulkCol <= action_col:
                action_col += 1

        return self.aadata(totalrows,
                           displayrows,
                           id,
                           draw,
                           flist,
                           action_col = action_col,
                           stringify = stringify,
                           **attr)

    # -------------------------------------------------------------------------
    # Extended API
    # -------------------------------------------------------------------------
    @staticmethod
    def getConfigData():
        """
            Method to extract the configuration data from S3 globals and
            store them as an attr variable.

            Returns:
                dictionary of attributes which can be passed into html()

            current.response.s3 parameters:
                dt_pageLength : The default number of records that will be shown
                dt_pagination: Enable pagination
                dt_pagingType: type of pagination, one of:
                                    simple
                                    simple_numbers
                                    full
                                    full_numbers (default)
                                http://datatables.net/reference/option/pagingType
                dt_searching: Enable or disable filtering of data.
                dt_group: The colum that is used to group the data
                dt_ajax_url: The URL to be used for the Ajax call
                dt_action_col: The column where the action buttons will be placed
                dt_bulk_actions: list of labels for the bulk actions.
                dt_bulk_col: The column in which the checkboxes will appear,
                             by default it will be the column immediately
                             before the first data item
                dt_bulk_selected: A list of selected items
                #dt_row_actions: a list of actions (each is a dict)
                dt_styles: dictionary of styles to be applied to a list of ids
                           for example:
                           {"warning" : [1,3,6,7,9], "alert" : [2,10,13]}
        """

        s3 = current.response.s3

        attr = Storage()
        if s3.datatable_ajax_source:
            attr.dt_ajax_url = s3.datatable_ajax_source
        # Defaults in htmlConfig() anyway:
        #if s3.actions:
        #    attr.dt_row_actions = s3.actions
        if s3.dataTableBulkActions:
            attr.dt_bulk_actions = s3.dataTableBulkActions
        if s3.dataTable_pageLength:
            attr.dt_pageLength = s3.dataTable_pageLength
        attr.dt_pagination = "false" if s3.no_sspag else "true"
        # Nothing using currently
        #if s3.dataTable_pagingType:
        #    attr.dt_pagingType = s3.dataTable_pagingType
        if s3.dataTable_group:
            attr.dt_group = s3.dataTable_group
        # Nothing using currently
        # - and not worth enabling as not used by standard CRUD
        #if s3.dataTable_NoSearch:
        #    attr.dt_searching = not s3.dataTable_NoSearch
        if s3.dataTable_dom:
            attr.dt_dom = s3.dataTable_dom
        if s3.dataTableDisplay:
            attr.dt_display = s3.dataTableDisplay
        if s3.dataTableStyleDisabled or s3.dataTableStyleWarning or s3.dataTableStyleAlert:
            attr.dt_styles = {}
            if s3.dataTableStyleDisabled:
                attr.dt_styles["dtdisable"] = s3.dataTableStyleDisabled
            if s3.dataTableStyleWarning:
                attr.dt_styles["dtwarning"] = s3.dataTableStyleWarning
            if s3.dataTableStyleAlert:
                attr.dt_styles["dtalert"] = s3.dataTableStyleAlert
        return attr

    # -------------------------------------------------------------------------
    @staticmethod
    def export_formats(rfields = None,
                       permalink = None,
                       base_url = None,
                       ):
        """
            Calculate the export formats that can be added to the table

            Args:
                rfields: optional list of field selectors for exports
                permalink: search result URL
                base_url: the base URL of the datatable (without
                          method or query vars) to construct format URLs
        """

        T = current.T
        s3 = current.response.s3
        request = current.request

        if base_url is None:
            base_url = request.url

        # @todo: other data formats could have other list_fields,
        #        so position-based datatable sorting/filters may
        #        be applied wrongly
        if s3.datatable_ajax_source:
            default_url = s3.datatable_ajax_source
        else:
            default_url = base_url

        # Strip format extensions (e.g. .aadata or .iframe)
        default_url = re.sub(r"(\/[a-zA-Z0-9_]*)(\.[a-zA-Z]*)", r"\g<1>", default_url)

        # Keep any URL filters
        get_vars = request.get_vars
        if get_vars:
            query = "&".join("%s=%s" % (k, v) for k, v in get_vars.items())
            default_url = "%s?%s" % (default_url, query)

        # Construct row of export icons
        # NB icons appear in reverse order due to float-right
        icons = SPAN(_class = "list_formats",
                     )

        settings = current.deployment_settings
        export_formats = settings.get_ui_export_formats()
        if export_formats:

            icons.append("%s:" % T("Export as"))

            formats = dict(s3.formats)

            # Auto-detect KML fields
            if "kml" not in formats and rfields:
                kml_fields = {"location_id", "site_id"}
                if any(rfield.fname in kml_fields for rfield in rfields):
                    formats["kml"] = default_url

            default_formats = ("xml", "rss", "xls", "pdf")
            EXPORT = T("Export in %(format)s format")

            append_icon = icons.append
            for fmt in export_formats:

                # CSS classes and on-hover title
                title = None
                if isinstance(fmt, tuple):
                    if len(fmt) >= 3:
                        title = fmt[2]
                    fmt, css = fmt[:2] if len(fmt) >= 2 else (fmt[0], "")
                else:
                    css = ""

                class_ = "dt-export export_%s" % fmt
                if css:
                    class_ = "%s %s" % (class_, css)

                if title is None:
                    if fmt == "map":
                        title = T("Show on Map")
                    else:
                        title = EXPORT % {"format": fmt.upper()}

                # Export format URL
                if fmt in default_formats:
                    url = formats.get(fmt, default_url)
                else:
                    url = formats.get(fmt)
                if not url:
                    continue

                append_icon(DIV(_class = class_,
                                _title = title,
                                data = {"url": url,
                                        "extension": fmt.split(".")[-1],
                                        },
                                ))

        export_options = DIV(_class = "dt-export-options",
                             )

        # Append the permalink (if any)
        if permalink is not None:
            label = settings.get_ui_label_permalink()
            if label:
                link = A(T(label),
                         _href = permalink,
                         _class = "permalink",
                         )
                export_options.append(link)
                if len(icons):
                    export_options.append(" | ")

        # Append the icons
        export_options.append(icons)

        return export_options

    # -------------------------------------------------------------------------
    @staticmethod
    def defaultActionButtons(resource,
                             custom_actions = None,
                             r = None
                             ):
        """
            Configure default action buttons

            Args:
                resource: the resource
                r: the request, if specified, all action buttons will
                   be linked to the controller/function of this request
                   rather than to prefix/name of the resource
                custom_actions: custom actions as list of dicts like
                                {"label":label, "url":url, "_class":class},
                                will be appended to the default actions

            TODO DRY with S3CRUD.action_buttons()
        """

        from ..methods import S3CRUD

        s3 = current.response.s3
        auth = current.auth
        actions = s3.actions = None

        table = resource.table
        has_permission = auth.s3_has_permission
        ownership_required = auth.permission.ownership_required

        labels = s3.crud_labels
        args = ["[id]"]

        # Choose controller/function to link to
        if r is not None:
            c = r.controller
            f = r.function
        else:
            c = resource.prefix
            f = resource.name

        tablename = resource.tablename
        get_config = current.s3db.get_config

        # "Open" button
        editable = get_config(tablename, "editable", True)
        if editable and has_permission("update", table) and \
           not ownership_required("update", table):
            update_url = URL(c=c, f=f, args=args + ["update"])
            S3CRUD.action_button(labels.UPDATE, update_url,
                                 icon = "edit",
                                 _class="action-btn edit")
        else:
            read_url = URL(c=c, f=f, args=args)
            S3CRUD.action_button(labels.READ, read_url,
                                 icon = "file",
                                 _class="action-btn read")

        # Delete button
        # @todo: does not apply selective action (renders DELETE for
        #        all items even if the user is only permitted to delete
        #        some of them) => should implement "restrict", see
        #        S3CRUD.action_buttons
        deletable = get_config(tablename, "deletable", True)
        if deletable and \
           has_permission("delete", table) and \
           not ownership_required("delete", table):
            delete_url = URL(c=c, f=f, args=args + ["delete"])
            S3CRUD.action_button(labels.DELETE, delete_url,
                                 icon = "delete",
                                 _class="delete-btn")

        # Append custom actions
        if custom_actions:
            actions = actions + custom_actions if actions else custom_actions

    # -------------------------------------------------------------------------
    @staticmethod
    def htmlConfig(html,
                   id,
                   orderby,
                   rfields = None,
                   cache = None,
                   **attr
                   ):
        """
            Method to wrap the html for a dataTable in a form, add the export formats
            and the config details required by dataTables

            Args:
                html: The html table
                id: The id of the table
                orderby: the sort details see http://datatables.net/reference/option/order
                rfields: The list of resource fields
                attr: dictionary of attributes which can be passed in

            Keyword Args:
                dt_lengthMenu: The menu options for the number of records to be shown
                dt_pageLength : The default number of records that will be shown
                dt_dom : The Datatable DOM initialisation variable, describing
                         the order in which elements are displayed.
                         See http://datatables.net/ref for more details.
                dt_pagination : Is pagination enabled, dafault 'true'
                dt_pagingType : How the pagination buttons are displayed
                dt_searching: Enable or disable filtering of data.
                dt_ajax_url: The URL to be used for the Ajax call
                dt_action_col: The column where the action buttons will be placed
                dt_bulk_actions: list of labels for the bulk actions.
                dt_bulk_col: The column in which the checkboxes will appear,
                             by default it will be the column immediately
                             before the first data item
                dt_bulk_single: only allow a single row to be selected
                dt_group: The column(s) that is(are) used to group the data
                dt_group_totals: The number of record in each group.
                                 This will be displayed in parenthesis
                                 after the group title.
                dt_group_titles: The titles to be used for each group.
                                 These are a list of lists with the inner list
                                 consisting of two values, the repr from the
                                 db and the label to display. This can be more than
                                 the actual number of groups (giving an empty group).
                dt_group_space: Insert a space between the group heading and the next group
                dt_bulk_selected: A list of selected items
                dt_row_actions: list of actions (each is a dict)
                dt_styles: dictionary of styles to be applied to a list of ids
                            for example:
                            {"warning" : [1,3,6,7,9], "alert" : [2,10,13]}
                dt_col_widths: dictionary of columns to apply a width to
                               for example: {1 : 15, 2 : 20}
                dt_text_maximum_len: The maximum length of text before it is condensed
                dt_text_condense_len: The length displayed text is condensed down to
                dt_double_scroll: Render double scroll bars (top+bottom), only available
                                  with settings.ui.datatables_responsive=False
                dt_shrink_groups: If set then the rows within a group will be hidden
                                  two types are supported, 'individual' and 'accordion'
                dt_group_types: The type of indicator for groups that can be 'shrunk'
                                Permitted valies are: 'icon' (the default) 'text' and 'none'
                dt_base_url: base URL to construct export format URLs, resource
                             default URL without any URL method or query part

            Note:
                current.response.s3.actions used to get the RowActions
        """

        from gluon.serializers import json as jsons

        request = current.request
        s3 = current.response.s3
        settings = current.deployment_settings

        dataTableID = s3.dataTableID
        if not dataTableID or not isinstance(dataTableID, list):
            dataTableID = s3.dataTableID = [id]
        elif id not in dataTableID:
            dataTableID.append(id)

        # The configuration parameter from the server to the client will be
        # sent in a json object stored in an hidden input field. This object
        # will then be parsed by s3.dataTable.js and the values used.
        config = Storage()
        config.id = id
        config.utf8 = False

        attr_get = attr.get
        config.dom = attr_get("dt_dom", settings.get_ui_datatables_dom())
        config.lengthMenu = attr_get("dt_lengthMenu",
                                     [[25, 50, -1],
                                      [25, 50, s3_str(current.T("All"))]
                                      ]
                                     )
        config.pageLength = attr_get("dt_pageLength", s3.ROWSPERPAGE)
        config.pagination = attr_get("dt_pagination", "true")
        config.pagingType = attr_get("dt_pagingType",
                                     settings.get_ui_datatables_pagingType())
        config.searching = attr_get("dt_searching", "true")

        ajaxUrl = attr_get("dt_ajax_url", None)
        if not ajaxUrl:
            url = URL(c = request.controller,
                      f = request.function,
                      args = request.args,
                      vars = request.get_vars,
                      )
            ajaxUrl = s3_set_extension(url, "aadata")
        config.ajaxUrl = ajaxUrl

        config.rowStyles = attr_get("dt_styles", [])

        colWidths = attr_get("dt_col_widths")
        if colWidths is not None:
            # NB This requires "table-layout:fixed" in your CSS
            # You will likely need to specify all column widths if you do this
            # & won't have responsiveness
            config.colWidths = colWidths

        rowActions = attr_get("dt_row_actions", s3.actions)
        if rowActions:
            config.rowActions = rowActions
        else:
            config.rowActions = []
        bulkActions = attr_get("dt_bulk_actions", None)
        if bulkActions and not isinstance(bulkActions, list):
            bulkActions = [bulkActions]
        config.bulkActions = bulkActions
        config.bulkCol = bulkCol = attr_get("dt_bulk_col", 0)
        if attr_get("dt_bulk_single"):
            config.bulkSingle = 1
        action_col = attr_get("dt_action_col", 0)
        if bulkActions and bulkCol <= action_col:
            action_col += 1
        config.actionCol = action_col

        group_list = attr_get("dt_group", [])
        if not isinstance(group_list, list):
            group_list = [group_list]
        dt_group = []
        for group in group_list:
            if bulkActions and bulkCol <= group:
                group += 1
            if action_col >= group:
                group -= 1
            dt_group.append([group, "asc"])
        config.group = dt_group
        config.groupTotals = attr_get("dt_group_totals", [])
        config.groupTitles = attr_get("dt_group_titles", [])
        config.groupSpacing = attr_get("dt_group_space")
        for order in orderby:
            if bulkActions:
                if bulkCol <= order[0]:
                    order[0] += 1
            if action_col > 0 and action_col >= order[0]:
                order[0] -= 1
        config.order = orderby
        config.textMaxLength = attr_get("dt_text_maximum_len", 80)
        config.textShrinkLength = attr_get("dt_text_condense_len", 75)
        config.shrinkGroupedRows = attr_get("dt_shrink_groups")
        config.groupIcon = attr_get("dt_group_types", [])

        # Activate double scroll and inject jQuery plugin
        if not settings.get_ui_datatables_responsive():
            double_scroll = attr_get("dt_double_scroll")
            if double_scroll is None:
                double_scroll = settings.get_ui_datatables_double_scroll()
            if double_scroll:
                if s3.debug:
                    script = "/%s/static/scripts/jquery.doubleScroll.js" % request.application
                else:
                    script = "/%s/static/scripts/jquery.doubleScroll.min.js" % request.application
                if script not in s3.scripts:
                    s3.scripts.append(script)
                html.add_class("doublescroll")

        # Wrap the table in a form and add some data in hidden fields
        form = FORM(_class="dt-wrapper")
        if not s3.no_formats:
            # @todo: move export-format update into drawCallback()
            # @todo: poor UX with onclick-JS, better to render real
            #        links which can be bookmarked, and then update them
            #        in drawCallback()
            permalink = attr_get("dt_permalink", None)
            base_url = attr_get("dt_base_url", None)
            export_formats = S3DataTable.export_formats(rfields,
                                                        permalink = permalink,
                                                        base_url = base_url)
            # Nb These can be moved around in initComplete()
            form.append(export_formats)

        form.append(html)

        # Add the configuration details for this dataTable
        form.append(INPUT(_type = "hidden",
                          _id = "%s_configurations" % id,
                          _name = "config",
                          _value = jsons(config),
                          ))

        # If we have a cache set up then pass it in
        if cache:
            form.append(INPUT(_type = "hidden",
                              _id = "%s_dataTable_cache" %id,
                              _name = "cache",
                              _value = jsons(cache),
                              ))

        # If we have bulk actions then add the hidden fields
        if bulkActions:
            form.append(INPUT(_type = "hidden",
                              _id = "%s_dataTable_bulkMode" % id,
                              _name = "mode",
                              _value = "Inclusive",
                              ))
            bulk_selected = attr_get("dt_bulk_selected", "")
            if isinstance(bulk_selected, list):
                bulk_selected = ",".join(bulk_selected)
            form.append(INPUT(_type = "hidden",
                              _id = "%s_dataTable_bulkSelection" % id,
                              _name = "selected",
                              _value = "[%s]" % bulk_selected,
                              ))
            form.append(INPUT(_type = "hidden",
                              _id = "%s_dataTable_filterURL" % id,
                              _class = "dataTable_filterURL",
                              _name = "filterURL",
                              _value = "%s" % config.ajaxUrl,
                              ))

        # Form key (CSRF protection for Ajax actions)
        formkey = attr_get("dt_formkey")
        if formkey:
            form["hidden"] = {"_formkey": formkey}

        # Set callback?
        initComplete = settings.get_ui_datatables_initComplete()
        if initComplete:
            # Processed in views/dataTables.html
            s3.dataTable_initComplete = initComplete

        return form

    # -------------------------------------------------------------------------
    # Helper methods
    # -------------------------------------------------------------------------
    def table(self, id, flist=None, action_col=0):
        """
            Method to render the data as an html table. This is of use if
            an html table is required without the dataTable goodness. However
            if you want html for a dataTable then use the html() method

            Args:
                id: The id of the table
                flist: The list of fields
                action_col: The column where action columns will be displayed
                            (this is required by dataTables)
        """

        data = self.data
        heading = self.heading
        start = self.start
        end = self.end
        if not flist:
            flist = self.colnames

        # Build the header row
        header = THEAD()
        tr = TR()
        for field in flist:
            if field == "BULK":
                tr.append(TH(""))
            else:
                tr.append(TH(heading[field]))
        header.append(tr)

        body = TBODY()
        if data:
            # Build the body rows (the actual data)
            rc = 0
            for i in range(start, end):
                row = data[i]
                if rc % 2 == 0:
                    _class = "even"
                else:
                    _class = "odd"
                rc += 1
                tr = TR(_class=_class)
                for field in flist:
                    # Insert a checkbox for bulk select
                    if field == "BULK":
                        tr.append(TD(INPUT(_type = "checkbox",
                                           _class = "bulkcheckbox",
                                           data = {"dbid": row[flist[action_col]]},
                                           )))
                    else:
                        tr.append(TD(row[field]))
                body.append(tr)
        table = TABLE([header, body],
                      _id = id,
                      _class = "dataTable display",
                      )

        if current.deployment_settings.get_ui_datatables_responsive():
            table.add_class("responsive")
        return table

    # -------------------------------------------------------------------------
    def aadata(self,
               totalrows,
               displayrows,
               id,
               draw,
               flist,
               stringify = True,
               action_col = None,
               **attr
               ):
        """
            Method to render the data into a json object

            Args:
                totalrows: The total rows in the unfiltered query.
                displayrows: The total rows in the filtered query.
                id: The id of the table for which this ajax call will
                    respond to.
                draw: An unaltered copy of draw sent from the client used
                      by dataTables as a draw count.
                flist: The list of fields
                attr: dictionary of attributes which can be passed in

            Keyword Args:
                dt_action_col: The column where the action buttons will be placed
                dt_bulk_actions: list of labels for the bulk actions.
                dt_bulk_col: The column in which the checkboxes will appear,
                             by default it will be the column immediately
                             before the first data item
                dt_group_totals: The number of record in each group.
                                 This will be displayed in parenthesis
                                 after the group title.
        """

        data = self.data
        if not flist:
            flist = self.colnames
        start = self.start
        end = self.end
        if action_col is None:
            action_col = attr.get("dt_action_col", 0)
        structure = {}
        aadata = []
        for i in range(start, end):
            row = data[i]
            details = []
            for field in flist:
                if field == "BULK":
                    details.append("<INPUT type='checkbox' class='bulkcheckbox' data-dbid='%s'>" % \
                                   row[flist[action_col]])
                else:
                    details.append(s3_str(row[field]))
            aadata.append(details)
        structure["dataTable_id"] = id # Is this used anywhere? Can't see it used, so could be removed?
        structure["dataTable_filter"] = self.filterString
        structure["dataTable_groupTotals"] = attr.get("dt_group_totals", [])
        structure["dataTable_sort"] = self.orderby
        structure["data"] = aadata
        structure["recordsTotal"] = totalrows
        structure["recordsFiltered"] = displayrows
        structure["draw"] = draw
        if stringify:
            from gluon.serializers import json as jsons
            return jsons(structure)
        else:
            return structure

# END =========================================================================
