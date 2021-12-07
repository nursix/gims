"""
    Interactive filters

    Copyright: 2013-2021 (c) Sahana Software Foundation

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

__all__ = ("S3FilterWidget",
           "S3AgeFilter",
           "S3DateFilter",
           "S3HierarchyFilter",
           "S3LocationFilter",
           "S3MapFilter",
           "S3OptionsFilter",
           "S3RangeFilter",
           "S3SliderFilter",
           "S3TextFilter",
           "S3NotEmptyFilter",
           "S3EmptyFilter",
           "S3FilterForm",
           "S3FilterString",
           "s3_get_filter_opts",
           "s3_set_default_filter",
           )

import datetime
import json
import re

from collections import OrderedDict

from gluon import current, URL, A, DIV, FORM, INPUT, LABEL, OPTION, SELECT, \
                  SPAN, TABLE, TAG, TBODY, \
                  IS_IN_SET
from gluon.storage import Storage

from s3dal import Field

from ..tools import IS_UTC_DATE, JSONSEPARATORS, S3DateTime, \
                    S3TypeConverter, s3_decode_iso_datetime, \
                    s3_get_foreign_key, s3_str
from ..ui import ICON, S3CalendarWidget, S3CascadeSelectWidget, \
                       S3GroupedOptionsWidget, S3HierarchyWidget, \
                       S3MultiSelectWidget
from ..resource import FS, S3ResourceField, S3ResourceQuery, S3URLQuery

# =============================================================================
class S3FilterWidget:
    """ Filter widget for interactive search forms (base class) """

    #: the HTML class for the widget type
    _class = "generic-filter"

    #: the default query operator(s) for the widget type
    operator = None

    #: alternatives for client-side changeable operators
    alternatives = None

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Prototype method to render this widget as an instance of
            a web2py HTML helper class, to be implemented by subclasses.

            Args:
                resource: the CRUDResource to render with widget for
                values: the values for this widget from the URL query
        """

        raise NotImplementedError

    # -------------------------------------------------------------------------
    def variable(self, resource, get_vars=None):
        """
            Prototype method to generate the name for the URL query variable
            for this widget, can be overwritten in subclasses.

            Args:
                resource: the resource

            Returns:
                the URL query variable name (or list of variable names if
                there are multiple operators)
        """

        opts = self.opts

        if "selector" in opts:
            # Override selector
            label, selector = None, opts["selector"]
        else:
            label, selector = self._selector(resource, self.field)
        self.selector = selector

        if not selector:
            return None

        if self.alternatives and get_vars is not None:
            # Get the actual operator from get_vars
            operator = self._operator(get_vars, selector)
            if operator:
                self.operator = operator

        if "label" not in self.opts:
            self.opts["label"] = label

        return self._variable(selector, self.operator)

    # -------------------------------------------------------------------------
    def data_element(self, variable):
        """
            Prototype method to construct the hidden element that holds the
            URL query term corresponding to an input element in the widget.

            Args:
                variable: the URL query variable
        """

        if type(variable) is list:
            variable = "&".join(variable)
        return INPUT(_type = "hidden",
                     _id = "%s-data" % self.attr["_id"],
                     _class = "filter-widget-data %s-data" % self._class,
                     _value = variable,
                     )

    # -------------------------------------------------------------------------
    # Helper methods
    #
    def __init__(self, field=None, **attr):
        """
            Constructor to configure the widget

            Args:
                field: the selector(s) for the field(s) to filter by
                attr: configuration options for this widget

            Keyword Args:
                label: label for the widget
                comment: comment for the widget
                hidden: render widget initially hidden (="advanced" option)

            - additional keywords see subclasses
        """

        self.field = field
        self.alias = None

        attributes = Storage()
        options = Storage()
        for k, v in attr.items():
            if k[0] == "_":
                attributes[k] = v
            else:
                options[k] = v
        self.attr = attributes
        self.opts = options

        self.selector = None
        self.values = Storage()

    # -------------------------------------------------------------------------
    def __call__(self, resource, get_vars=None, alias=None):
        """
            Entry point for the form builder

            Args:
                resource: the CRUDResource to render the widget for
                get_vars: the GET vars (URL query vars) to prepopulate
                          the widget
                alias: the resource alias to use
        """

        self.alias = alias

        # Initialize the widget attributes
        self._attr(resource)

        # Extract the URL values to populate the widget
        variable = self.variable(resource, get_vars)

        defaults = {}
        for k, v in self.values.items():
            selector = self._prefix(k)
            defaults[selector] = v

        if type(variable) is list:
            values = Storage()
            for k in variable:
                if k in defaults:
                    values[k] = defaults[k]
                else:
                    values[k] = self._values(get_vars, k)
        else:
            if variable in defaults:
                values = defaults[variable]
            else:
                values = self._values(get_vars, variable)

        # Construct and populate the widget
        widget = self.widget(resource, values)

        # Recompute variable in case operator got changed in widget()
        if self.alternatives:
            variable = self._variable(self.selector, self.operator)

        # Construct the hidden data element
        data = self.data_element(variable)

        if type(data) is list:
            data.append(widget)
        else:
            data = [data, widget]
        return TAG[""](*data)

    # -------------------------------------------------------------------------
    def _attr(self, resource):
        """ Initialize and return the HTML attributes for this widget """

        _class = self._class

        # Construct name and id for the widget
        attr = self.attr
        if "_name" not in attr:
            if not resource:
                raise SyntaxError("%s: _name parameter required " \
                                  "when rendered without resource." % \
                                  self.__class__.__name__)
            flist = self.field
            if not isinstance(flist, (list, tuple)):
                flist = [flist]
            colnames = []
            for f in flist:
                rfield = S3ResourceField(resource, f)
                colname = rfield.colname
                if colname:
                    colnames.append(colname)
                else:
                    colnames.append(rfield.fname)
            name = "%s-%s-%s" % (resource.alias, "-".join(colnames), _class)
            attr["_name"] = name.replace(".", "_")
        if "_id" not in attr:
            attr["_id"] = attr["_name"]

        return attr

    # -------------------------------------------------------------------------
    @classmethod
    def _operator(cls, get_vars, selector):
        """
            Helper method to get the operators from the URL query

            Args:
                get_vars: the GET vars (a dict)
                selector: field selector

            Returns:
                query operator - None, str or list
        """

        variables = ["%s__%s" % (selector, op) for op in cls.alternatives]
        slen = len(selector) + 2

        operators = [k[slen:] for k in get_vars if k in variables]
        if not operators:
            return None
        elif len(operators) == 1:
            return operators[0]
        else:
            return operators

    # -------------------------------------------------------------------------
    def _prefix(self, selector):
        """
            Helper method to prefix an unprefixed field selector

            Args:
                alias: the resource alias to use as prefix
                selector: the field selector

            Returns:
                the prefixed selector
        """

        alias = self.alias
        items = selector.split("$", 0)
        head = items[0]
        if "." in head:
            if alias not in (None, "~"):
                prefix, key = head.split(".", 1)
                if prefix == "~":
                    prefix = alias
                elif prefix != alias:
                    prefix = "%s.%s" % (alias, prefix)
                items[0] = "%s.%s" % (prefix, key)
                selector = "$".join(items)
        else:
            if alias is None:
                alias = "~"
            selector = "%s.%s" % (alias, selector)
        return selector

    # -------------------------------------------------------------------------
    def _selector(self, resource, fields):
        """
            Helper method to generate a filter query selector for the
            given field(s) in the given resource.

            Args:
                resource: the CRUDResource
                fields: the field selectors (as strings)

            Returns:
                the field label and the filter query selector, or None
                if none of the field selectors could be resolved
        """

        prefix = self._prefix
        label = None

        if not fields:
            return label, None
        if not isinstance(fields, (list, tuple)):
            fields = [fields]
        selectors = []
        for field in fields:
            if resource:
                try:
                    rfield = S3ResourceField(resource, field)
                except (AttributeError, TypeError):
                    continue
                if not rfield.field and not rfield.virtual:
                    # Unresolvable selector
                    continue
                if not label:
                    label = rfield.label
                selectors.append(prefix(rfield.selector))
            else:
                selectors.append(field)
        if selectors:
            return label, "|".join(selectors)
        else:
            return label, None

    # -------------------------------------------------------------------------
    @staticmethod
    def _values(get_vars, variable):
        """
            Helper method to get all values of a URL query variable

            Args:
                get_vars: the GET vars (a dict)
                variable: the name of the query variable

            Returns:
                a list of values
        """

        if not variable:
            return []
        elif variable in get_vars:
            values = S3URLQuery.parse_value(get_vars[variable])
            if not isinstance(values, (list, tuple)):
                values = [values]
            return values
        else:
            return []

    # -------------------------------------------------------------------------
    @classmethod
    def _variable(cls, selector, operator):
        """
            Construct URL query variable(s) name from a filter query
            selector and the given operator(s)

            Args:
                selector: the selector
                operator: the operator (or tuple/list of operators)

            Returns:
                the URL query variable name (or list of variable names)
        """

        if isinstance(operator, (tuple, list)):
            return [cls._variable(selector, o) for o in operator]
        elif operator:
            return "%s__%s" % (selector, operator)
        else:
            return selector

# =============================================================================
class S3TextFilter(S3FilterWidget):
    """
        Text filter widget

        Keyword Args:
            label: label for the widget
            comment: comment for the widget
            hidden: render widget initially hidden (="advanced" option)
            match_any: match any of the strings
    """

    _class = "text-filter"

    operator = "like"

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        attr = self.attr

        if "_size" not in attr:
            attr.update(_size="40")
        if "_class" in attr and attr["_class"]:
            _class = "%s %s" % (attr["_class"], self._class)
        else:
            _class = self._class
        attr["_class"] = _class
        attr["_type"] = "text"

        # Match any or all of the strings entered?
        data = attr.get("data", {})
        data["match"] = "any" if self.opts.get("match_any") else "all"
        attr["data"] = data

        values = [v.strip("*") for v in values if v is not None]
        if values:
            attr["_value"] = " ".join(values)

        return INPUT(**attr)

# =============================================================================
class S3RangeFilter(S3FilterWidget):
    """
        Numerical Range Filter Widget

        Keyword Args:
            label: label for the widget
            comment: comment for the widget
            hidden: render widget initially hidden (="advanced" option)
    """

    # Overall class
    _class = "range-filter"
    # Class for visible input boxes.
    _input_class = "%s-%s" % (_class, "input")

    operator = ["ge", "le"]

    # Untranslated labels for individual input boxes.
    input_labels = {"ge": "Minimum", "le": "Maximum"}

    # -------------------------------------------------------------------------
    def data_element(self, variables):
        """
            Overrides S3FilterWidget.data_element(), constructs multiple
            hidden INPUTs (one per variable) with element IDs of the form
            <id>-<operator>-data (where no operator is translated as "eq").

            Args:
                variables: the variables
        """

        if variables is None:
            operators = self.operator
            if type(operators) is not list:
                operators = [operators]
            variables = self._variable(self.selector, operators)
        else:
            # Split the operators off the ends of the variables.
            if type(variables) is not list:
                variables = [variables]
            parse_key = S3URLQuery.parse_key
            operators = [parse_key(v)[1] for v in variables]

        elements = []
        widget_id = self.attr["_id"]

        for o, v in zip(operators, variables):
            elements.append(
                INPUT(_type = "hidden",
                      _id = "%s-%s-data" % (widget_id, o),
                      _class = "filter-widget-data %s-data" % self._class,
                      _value = v,
                      ))

        return elements

    # -------------------------------------------------------------------------
    def ajax_options(self, resource):
        """
            Method to Ajax-retrieve the current options of this widget

            Args:
                resource: the CRUDResource
        """

        minimum, maximum = self._options(resource)

        attr = self._attr(resource)
        options = {attr["_id"]: {"min": minimum,
                                 "max": maximum,
                                 }}
        return options

    # -------------------------------------------------------------------------
    def _options(self, resource):
        """
            Helper function to retrieve the current options for this
            filter widget

            Args:
                resource: the CRUDResource
        """

        # Find only values linked to records the user is
        # permitted to read, and apply any resource filters
        # (= use the resource query)
        query = resource.get_query()

        # Must include rfilter joins when using the resource
        # query (both inner and left):
        rfilter = resource.rfilter
        if rfilter:
            join = rfilter.get_joins()
            left = rfilter.get_joins(left = True)
        else:
            join = left = None

        rfield = S3ResourceField(resource, self.field)
        field = rfield.field

        row = current.db(query).select(field.min(),
                                       field.max(),
                                       join = join,
                                       left = left,
                                       ).first()

        minimum = row[field.min()]
        maximum = row[field.max()]

        return minimum, maximum

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        T = current.T

        attr = self.attr
        _class = self._class
        if "_class" in attr and attr["_class"]:
            _class = "%s %s" % (attr["_class"], _class)
        else:
            _class = _class
        attr["_class"] = _class

        input_class = self._input_class
        input_labels = self.input_labels
        input_elements = DIV(_class="range-filter")
        ie_append = input_elements.append

        _id = attr["_id"]
        _variable = self._variable
        selector = self.selector

        for operator in self.operator:

            input_id = "%s-%s" % (_id, operator)

            input_box = INPUT(_name = input_id,
                              _id = input_id,
                              _type = "text",
                              _class = input_class,
                              )

            variable = _variable(selector, operator)

            # Populate with the value, if given
            # if user has not set any of the limits, we get [] in values.
            value = values.get(variable, None)
            if value not in [None, []]:
                if type(value) is list:
                    value = value[0]
                input_box["_value"] = value
                input_box["value"] = value

            ie_append(DIV(DIV(LABEL("%s:" % T(input_labels[operator]),
                                    _for = input_id,
                                    ),
                              _class = "range-filter-label",
                              ),
                          DIV(input_box,
                              _class = "range-filter-widget",
                              ),
                          _class = "range-filter-field",
                          ))

        return input_elements

# =============================================================================
class S3AgeFilter(S3RangeFilter):

    _class = "age-filter"

    # Class for visible input boxes.
    _input_class = "%s-%s" % (_class, "input")

    operator = ["le", "gt"]

    # Untranslated labels for individual input boxes.
    input_labels = {"le": "", "gt": "To"}

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        T = current.T

        attr = self.attr
        _class = self._class
        if "_class" in attr and attr["_class"]:
            _class = "%s %s" % (attr["_class"], _class)
        else:
            _class = _class
        attr["_class"] = _class

        input_class = self._input_class
        input_labels = self.input_labels
        input_elements = DIV()
        ie_append = input_elements.append

        _id = attr["_id"]
        _variable = self._variable
        selector = self.selector

        opts = self.opts
        minimum = opts.get("minimum", 0)
        maximum = opts.get("maximum", 120)

        for operator in self.operator:

            input_id = "%s-%s" % (_id, operator)

            # Selectable options
            input_opts = [OPTION("%s" % i, value=i)
                          for i in range(minimum, maximum + 1)
                          ]
            input_opts.insert(0, OPTION("", value=""))

            # Input Element
            input_box = SELECT(input_opts,
                               _id = input_id,
                               _class = input_class,
                               )

            variable = _variable(selector, operator)

            # Populate with the value, if given
            # if user has not set any of the limits, we get [] in values.
            value = values.get(variable, None)
            if value not in [None, []]:
                if type(value) is list:
                    value = value[0]
                input_box["_value"] = value
                input_box["value"] = value

            label = input_labels[operator]
            if label:
                label = DIV(LABEL("%s:" % T(input_labels[operator]),
                                  _for = input_id,
                                  ),
                            _class = "age-filter-label",
                            )

            ie_append(DIV(label,
                          DIV(input_box,
                              _class = "age-filter-widget",
                              ),
                          _class = "range-filter-field",
                          ))

        ie_append(DIV(LABEL(T("Years")),
                      _class = "age-filter-unit",
                      # TODO move style into CSS
                      #_style = "float:left;margin-top:1.2rem;vertical-align:text-bottom",
                      ))

        return input_elements

# =============================================================================
class S3DateFilter(S3RangeFilter):
    """
        Date Range Filter Widget
            - use a single field or a pair of fields for start_date/end_date

        Keyword Args:
            label: label for the widget
            comment: comment for the widget
            hidden: render widget initially hidden (="advanced" option)
            fieldtype: explicit field type "date" or "datetime" to
                       use for context or virtual fields
            hide_time: don't show time selector

            WIP/incomplete:
            filterby: field to filter records included by
            filter_opts: options to filter records included by
            negative: exclude matching records rather than including them,
                      provide the selector for the "selector=None"
    """

    _class = "date-filter"

    # Class for visible input boxes.
    _input_class = "%s-%s" % (_class, "input")

    #operator = ["ge", "le"]

    # Untranslated labels for individual input boxes.
    input_labels = {"ge": "From", "le": "To"}

    # -------------------------------------------------------------------------
    def __call__(self, resource, get_vars=None, alias=None):
        """
            Entry point for the form builder
            - subclassed from S3FilterWidget to handle 'available' selector

            Args:
                resource: the CRUDResource to render the widget for
                get_vars: the GET vars (URL query vars) to prepopulate
                          the widget
                alias: the resource alias to use
        """

        self.alias = alias

        # Initialize the widget attributes
        self._attr(resource)

        # Extract the URL values to populate the widget
        variable = self.variable(resource, get_vars)

        defaults = {}
        for k, v in self.values.items():
            if k.startswith("available"):
                selector = k
            else:
                selector = self._prefix(k)
            defaults[selector] = v

        if type(variable) is list:
            values = Storage()
            for k in variable:
                if k in defaults:
                    values[k] = defaults[k]
                else:
                    values[k] = self._values(get_vars, k)
        else:
            if variable in defaults:
                values = defaults[variable]
            else:
                values = self._values(get_vars, variable)

        # Construct and populate the widget
        widget = self.widget(resource, values)

        # Recompute variable in case operator got changed in widget()
        if self.alternatives:
            variable = self._variable(self.selector, self.operator)

        # Construct the hidden data element
        data = self.data_element(variable)

        if type(data) is list:
            data.append(widget)
        else:
            data = [data, widget]
        return TAG[""](*data)

    # -------------------------------------------------------------------------
    def data_element(self, variables):
        """
            Overrides S3FilterWidget.data_element(), constructs multiple
            hidden INPUTs (one per variable) with element IDs of the form
            <id>-<operator>-data (where no operator is translated as "eq").

            Args:
                variables: the variables
        """

        fields = self.field
        if type(fields) is not list:
            # Use function from S3RangeFilter parent class
            return super(S3DateFilter, self).data_element(variables)

        selectors = self.selector.split("|")
        operators = self.operator

        elements = []
        _id = self.attr["_id"]

        start = True
        for selector in selectors:
            if start:
                operator = operators[0]
                start = False
            else:
                operator = operators[1]
            variable = self._variable(selector, [operator])[0]

            elements.append(
                INPUT(_type = "hidden",
                      _id = "%s-%s-data" % (_id, operator),
                      _class = "filter-widget-data %s-data" % self._class,
                      _value = variable,
                      ))

        return elements

    # -------------------------------------------------------------------------
    def ajax_options(self, resource):
        """
            Method to Ajax-retrieve the current options of this widget

            Args:
                resource: the CRUDResource
        """

        # Introspective range?
        auto_range = self.opts.get("auto_range")
        if auto_range is None:
            # Not specified for widget => apply global setting
            auto_range = current.deployment_settings.get_search_dates_auto_range()

        if auto_range:
            minimum, maximum, ts = self._options(resource)

            attr = self._attr(resource)
            options = {attr["_id"]: {"min": minimum,
                                     "max": maximum,
                                     "ts": ts,
                                     }}
        else:
            options = {}

        return options

    # -------------------------------------------------------------------------
    def _options(self, resource, as_str=True):
        """
            Helper function to retrieve the current options for this
            filter widget

            Args:
                resource: the CRUDResource
                as_str: return date as ISO-formatted string not raw DateTime

            TODO Update for negative
        """

        # Find only values linked to records the user is
        # permitted to read, and apply any resource filters
        # (= use the resource query)
        query = resource.get_query()

        filterby = self.opts.get("filterby")
        if filterby:
            filter_opts = self.opts.get("filter_opts")
            if filter_opts:
                if not isinstance(filter_opts, (list, tuple)):
                    query &= (FS(filterby) == filter_opts)
                elif len(filter_opts) == 1:
                    query &= (FS(filterby) == filter_opts[0])
                else:
                    query &= (FS(filterby).belongs(filter_opts))

        # Must include rfilter joins when using the resource
        # query (both inner and left):
        rfilter = resource.rfilter
        if rfilter:
            join = rfilter.get_joins()
            left = rfilter.get_joins(left = True)
        else:
            join = left = None

        fields = self.field
        if type(fields) is list:
            # Separate start_date & end_date
            # Q: How should we handle NULL end_date?
            # A: Ignore & just provide constraints that provide differentiation?
            # B: Allow scrolling arbitrarily into the end space?
            # Going with (A) for now
            # If wanting to do (B) then can coalesce a long end_date to the NULLs:
            # http://stackoverflow.com/questions/21286215/how-can-i-include-null-values-in-a-min-or-max
            # http://www.web2py.com/books/default/chapter/29/06/the-database-abstraction-layer#Default-values-with-coalesce-and-coalesce_zero
            # or can simply do a 2nd query to check for NULLs
            # client-side JS does an OR end_field is None
            start_field = S3ResourceField(resource, fields[0]).field
            end_field = S3ResourceField(resource, fields[1]).field
            row = current.db(query).select(start_field.min(),
                                           start_field.max(),
                                           end_field.max(),
                                           join = join,
                                           left = left,
                                           ).first()
            minimum = row[start_field.min()]
            maximum = row[start_field.max()]
            end_max = row[end_field.max()]
            if end_max:
                maximum = max(maximum, end_max)
        else:
            rfield = S3ResourceField(resource, fields)
            field = rfield.field
            row = current.db(query).select(field.min(),
                                           field.max(),
                                           join = join,
                                           left = left,
                                           ).first()
            minimum = row[field.min()]
            maximum = row[field.max()]

        # Ensure that we can select the extreme values
        minute_step = 5
        timedelta = datetime.timedelta
        if minimum:
            minimum -= timedelta(minutes = minute_step)
        if maximum:
            maximum += timedelta(minutes = minute_step)

        # @ToDo: separate widget/deployment_setting
        if self.opts.get("slider"):
            if type(fields) is list:
                event_start = fields[0]
                event_end = fields[0]
            else:
                event_start = event_end = fields
            from ..tools import TimeSeries
            ts = TimeSeries(resource,
                            start = minimum,
                            end = maximum,
                            slots = None,     # Introspect the data
                            event_start = event_start,
                            event_end = event_end,
                            rows = None,
                            cols = None,
                            facts = None,     # Default to Count id
                            baseline = None,  # No baseline
                            )
            # Extract aggregated results as JSON-serializable dict
            data = ts.as_dict()
            # We just want the dates & values
            data = data["p"]
            #ts = [(v["t"][0], v["t"][1], v["v"][0]) for v in data] # If we send start & end of slots
            ts = [(v["t"][0], v["v"][0]) for v in data]
        else:
            ts = []

        if as_str:
            ISO = "%Y-%m-%dT%H:%M:%S"
            if minimum:
                minimum = minimum.strftime(ISO)
            if maximum:
                maximum = maximum.strftime(ISO)

        return minimum, maximum, ts

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        attr = self.attr
        opts_get = self.opts.get

        # CSS class and element ID
        _class = self._class
        if "_class" in attr and attr["_class"]:
            _class = "%s %s" % (attr["_class"], _class)
        else:
            _class = _class
        _id = attr["_id"]

        # Picker options
        clear_text = opts_get("clear_text", None)
        hide_time = opts_get("hide_time", False)

        # Introspective range?
        slider = opts_get("slider", False)
        if slider:
            # Default to True
            auto_range = opts_get("auto_range", True)
        else:
            auto_range = opts_get("auto_range")
            if auto_range is None:
                # Not specified for widget => apply global setting
                auto_range = current.deployment_settings.get_search_dates_auto_range()

        if auto_range:
            minimum, maximum, ts = self._options(resource, as_str=False)
        else:
            minimum = maximum = None

        # Generate the input elements
        filter_widget = DIV(_id = _id,
                            _class = _class,
                            )
        append = filter_widget.append

        # Classes and labels for the individual date/time inputs
        T = current.T
        input_class = self._input_class
        negative = opts_get("negative", None)
        if negative is not None:
            input_class = "%s %s" % (input_class, "negative")
            append(INPUT(_id = "%s-negative" % _id,
                         _type = "hidden",
                         _value = negative,
                         ))
        input_labels = self.input_labels

        filterby = opts_get("filterby", None)
        if filterby is not None:
            filter_opts = opts_get("filter_opts", None)
            if filter_opts is not None:
                append(INPUT(_id = "%s-filterby" % _id,
                             _type = "hidden",
                             _value = filterby,
                             ))
                append(INPUT(_id = "%s-filter_opts" % _id,
                             _type = "hidden",
                             # @ToDo: Each option needs to be wrapped in ""
                             _value = ",".join(filter_opts),
                             ))

        if slider:
            # Load Moment & D3/NVD3 into Browser
            # @ToDo: Set Moment locale
            # NB This will probably get used more widely in future, so maybe need to abstract this somewhere else
            appname = current.request.application
            s3 = current.response.s3
            scripts_append = s3.scripts.append
            if s3.debug:
                if s3.cdn:
                    scripts_append("https://cdnjs.cloudflare.com/ajax/libs/moment.js/2.18.1/moment.js")
                    scripts_append("https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.17/d3.js")
                    # We use a patched v1.8.5 currently, so can't use the CDN version
                    #scripts_append("https://cdnjs.cloudflare.com/ajax/libs/nvd3/1.8.5/nv.d3.js")
                else:
                    scripts_append("/%s/static/scripts/moment.js" % appname)
                    scripts_append("/%s/static/scripts/d3/d3.js" % appname)
                scripts_append("/%s/static/scripts/d3/nv.d3.js" % appname)
            else:
                if s3.cdn:
                    scripts_append("https://cdnjs.cloudflare.com/ajax/libs/moment.js/2.18.1/moment.min.js")
                    scripts_append("https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.17/d3.min.js")
                    #scripts_append("https://cdnjs.cloudflare.com/ajax/libs/nvd3/1.8.5/nv.d3.min.js")
                else:
                    scripts_append("/%s/static/scripts/moment.min.js" % appname)
                    scripts_append("/%s/static/scripts/d3/d3.min.js" % appname)
                scripts_append("/%s/static/scripts/d3/nv.d3.min.js" % appname)
            range_picker = DIV(_class="range-picker")
            ISO = "%Y-%m-%dT%H:%M:%S"
            if minimum:
                range_picker["_data-min"] = minimum.strftime(ISO)
            if maximum:
                range_picker["_data-max"] = maximum.strftime(ISO)
            if ts:
                range_picker["_data-ts"] = json.dumps(ts, separators=JSONSEPARATORS)
            if hide_time:
                # @ToDo: Translate Settings from Python to Moment
                # http://momentjs.com/docs/#/displaying/
                # https://github.com/benjaminoakes/moment-strftime
                # range_picker["_data-fmt"] = current.deployment_settings.get_L10n_date_format()
                range_picker["_data-fmt"] = "MMM D YYYY"
                #range_picker["_data-fmt"] = "LL" # Locale-aware version
            else:
                #range_picker["_data-fmt"] = current.deployment_settings.get_L10n_datetime_format()
                range_picker["_data-fmt"] = "MMM D YYYY HH:mm"
                #range_picker["_data-fmt"] = "LLL" # Locale-aware version
            append(DIV(range_picker,
                       _class = "range-picker-wrapper",
                       ))

        get_variable = self._variable

        fields = self.field
        if type(fields) is not list:
            fields = [fields]
            selector = self.selector
        else:
            selectors = self.selector.split("|")

        start = True
        for field in fields:
            # Determine the field type
            if resource:
                rfield = S3ResourceField(resource, field)
                field = rfield.field
            else:
                rfield = field = None
            if not field:
                if not rfield or rfield.virtual:
                    ftype = opts_get("fieldtype", "datetime")
                else:
                    # Unresolvable selector
                    return ""
            else:
                ftype = rfield.ftype

            # S3CalendarWidget requires a Field
            if not field:
                if rfield:
                    tname, fname = rfield.tname, rfield.fname
                else:
                    tname, fname = "notable", "datetime"
                    if not _id:
                        raise SyntaxError("%s: _id parameter required " \
                                          "when rendered without resource." % \
                                          self.__class__.__name__)
                field = Field(fname, ftype, requires = IS_UTC_DATE())
                field.tablename = field._tablename = tname

            if len(fields) == 1:
                operators = self.operator
            else:
                # 2 Separate fields
                if start:
                    operators = ["ge"]
                    selector = selectors[0]
                    start = False
                else:
                    operators = ["le"]
                    selector = selectors[1]
                    input_class += " end_date"

            # Do we want a timepicker?
            timepicker = False if ftype == "date" or hide_time else True
            if timepicker and "datetimepicker" not in input_class:
                input_class += " datetimepicker"
            if ftype != "date" and hide_time:
                # Indicate that this filter is for a datetime field but
                # with a hidden time selector (so it shall add a suitable
                # time fragment automatically)
                input_class += " hide-time"

            for operator in operators:

                input_id = "%s-%s" % (_id, operator)

                # Make the two inputs constrain each other
                set_min = set_max = None
                if operator == "ge":
                    set_min = "#%s-%s" % (_id, "le")
                elif operator == "le":
                    set_max = "#%s-%s" % (_id, "ge")

                # Instantiate the widget
                widget = S3CalendarWidget(timepicker = timepicker,
                                          minimum = minimum,
                                          maximum = maximum,
                                          set_min = set_min,
                                          set_max = set_max,
                                          clear_text = clear_text,
                                          )

                # Populate with the value, if given
                # if user has not set any of the limits, we get [] in values.
                value = values.get(get_variable(selector, operator))
                if value in (None, []):
                    value = None
                elif type(value) is list:
                    value = value[0]

                # Widget expects a string in local calendar and format
                if isinstance(value, str):
                    # URL filter or filter default come as string in
                    # Gregorian calendar and ISO format => convert into
                    # a datetime
                    try:
                        dt = s3_decode_iso_datetime(value)
                    except ValueError:
                        dt = None
                else:
                    # Assume datetime
                    dt = value
                if dt:
                    if timepicker:
                        dtstr = S3DateTime.datetime_represent(dt, utc=False)
                    else:
                        dtstr = S3DateTime.date_represent(dt, utc=False)
                else:
                    dtstr = None

                # Render the widget
                picker = widget(field,
                                dtstr,
                                _class = input_class,
                                _id = input_id,
                                _name = input_id,
                                )

                if operator in input_labels:
                    label = DIV(LABEL("%s:" % T(input_labels[operator]),
                                      _for = input_id,
                                      ),
                                _class = "range-filter-label",
                                )
                else:
                    label = ""

                # Append label and widget
                append(DIV(label,
                           DIV(picker,
                               _class = "range-filter-widget",
                               ),
                           _class = "range-filter-field",
                           ))

        return filter_widget

# =============================================================================
class S3SliderFilter(S3RangeFilter):
    """
        Filter widget for Ranges which is controlled by a Slider instead of
        INPUTs, wraps jQueryUI's Range Slider in S3.range_slider in S3.js

        Keyword Args:
            label: label for the widget
            comment: comment for the widget
            hidden: render widget initially hidden (="advanced" option)
    """

    # -------------------------------------------------------------------------
    def ajax_options(self, resource):
        """
            Method to Ajax-retrieve the current options of this widget

            Args:
                resource: the CRUDResource
        """

        minimum, maximum = self._options(resource)[:2]

        attr = self._attr(resource)
        options = {attr["_id"]: {"min": minimum,
                                 "max": maximum,
                                 }}
        return options

    # -------------------------------------------------------------------------
    def _options(self, resource):
        """
            Helper function to retrieve the current options for this
            filter widget

            Args:
                resource: the CRUDResource
        """

        db = current.db

        # Find only values linked to records the user is
        # permitted to read, and apply any resource filters
        # (= use the resource query)
        query = resource.get_query()

        # Must include rfilter joins when using the resource
        # query (both inner and left):
        rfilter = resource.rfilter
        if rfilter:
            join = rfilter.get_joins()
            left = rfilter.get_joins(left = True)
        else:
            join = left = None

        rfield = S3ResourceField(resource, self.field)
        field = rfield.field

        row = db(query).select(field.min(),
                               field.max(),
                               join = join,
                               left = left,
                               ).first()

        minimum = row[field.min()]
        maximum = row[field.max()]

        empty = db(query & (field == None)).select(resource.table.id,
                                                   limitby = (0, 1)
                                                   ).first()

        return minimum, maximum, empty

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        T = current.T

        attr = self.attr
        _class = self._class
        if "_class" in attr and attr["_class"]:
            _class = "%s %s" % (attr["_class"], _class)
        else:
            _class = _class
        attr["_class"] = _class

        _id = attr["_id"]
        field = self.field
        input_class = self._input_class
        input_labels = self.input_labels
        _variable = self._variable
        selector = self.selector

        input_elements = DIV()
        ie_append = input_elements.append

        slider_id = "%s_slider" % field
        slider = DIV(_id = slider_id)
        ie_append(slider)
        input_ids = []
        _values = []

        opts_get = self.opts.get
        step = opts_get("step", 1)
        _type = opts_get("type", "int")

        # Set min_value & max_value
        #min_value = opts_get("minimum", None)
        #max_value = opts_get("maximum", None)
        #if min_value is None or max_value is None:
        # Introspect from the data
        min_value, max_value, empty = self._options(resource)
        if min_value is None:
            #min_value = "null"
            min_value = 0
        elif empty is not None and \
             min_value > 0:
            # Treat None as 0
            min_value = 0
        if max_value is None:
            #max_value = "null"
            max_value = 0

        for operator in self.operator:

            input_id = "%s-%s" % (_id, operator)
            input_ids.append(input_id)

            input_box = INPUT(_name = input_id,
                              _id = input_id,
                              _type = "text",
                              _class = input_class,
                              )

            variable = _variable(selector, operator)

            # Populate with the value, if given
            # if user has not set any of the limits, we get [] in values.
            value = values.get(variable, None)
            if value not in [None, []]:
                if type(value) is list:
                    value = value[0]
                input_box["_value"] = value
                input_box["value"] = value
                _values.append(value)
            else:
                if operator == "ge":
                    _values.append(min_value)
                else:
                    _values.append(max_value)

            ie_append(DIV(DIV(LABEL("%s:" % T(input_labels[operator]),
                                    _for = input_id,
                                    ),
                              _class = "range-filter-label",
                              ),
                          DIV(input_box,
                              _class = "range-filter-widget",
                              ),
                          _class = "range-filter-field",
                          ))

        s3 = current.response.s3

        script = '''i18n.slider_help="%s"''' % \
            current.T("Click on the slider to choose a value")
        s3.js_global.append(script)

        if _type == "int":
            script = '''S3.range_slider('%s','%s','%s',%i,%i,%i,[%i,%i])''' % \
                        (slider_id,
                         input_ids[0],
                         input_ids[1],
                         min_value,
                         max_value,
                         step,
                         _values[0],
                         _values[1],
                         )
        else:
            # Float
            script = '''S3.range_slider('%s','%s','%s',%f,%f,%f,[%i,%i])''' % \
                        (slider_id,
                         input_ids[0],
                         input_ids[1],
                         min_value,
                         max_value,
                         step,
                         _values[0],
                         _values[1],
                         )
        s3.jquery_ready.append(script)

        return input_elements

# =============================================================================
class S3LocationFilter(S3FilterWidget):
    """
        Hierarchical Location Filter Widget

        Keyword Args:
            ** Widget appearance:
            label: label for the widget
            comment: comment for the widget
            hidden: render widget initially hidden (="advanced" option)
            no_opts: text to show if no options available

            ** Options-lookup:
            levels: list of location hierarchy levels
            resource: alternative resource to look up options
            lookup: field in the alternative resource to look up
            options: fixed set of options (list of gis_location IDs)

            ** Multiselect-dropdowns:
            search: show search-field to search for options
            header: show header with bulk-actions
            selectedList: number of selected items to show on
                          button before collapsing into number of items

        Note:
            This will show records linked to all child locations of the Lx
    """

    _class = "location-filter"

    operator = "belongs"

    # -------------------------------------------------------------------------
    def __init__(self, field=None, **attr):
        """
            Constructor to configure the widget

            Args:
                field: the selector(s) for the field(s) to filter by
                attr: configuration options for this widget
        """

        if not field:
            field = "location_id"

        # Translate options using gis_location_name?
        settings = current.deployment_settings
        translate = settings.get_L10n_translate_gis_location()
        if translate:
            language = current.session.s3.language
            #if language == settings.get_L10n_default_language():
            if language == "en": # Can have a default language for system & yet still want to translate from base English
                translate = False
        self.translate = translate

        super(S3LocationFilter, self).__init__(field=field, **attr)

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        attr = self._attr(resource)
        opts = self.opts
        name = attr["_name"]

        ftype, levels, noopt = self._options(resource, values=values)
        if noopt:
            return SPAN(noopt, _class="no-options-available")

        # Filter class (default+custom)
        _class = self._class
        if "_class" in attr and attr["_class"]:
            _class = "%s %s" % (_class, attr["_class"])
        attr["_class"] = _class

        # Store id and name for the data element
        base_id = attr["_id"]
        base_name = attr["_name"]

        widgets = []
        w_append = widgets.append
        operator = self.operator
        field_name = self.field

        fname = self._prefix(field_name) if resource else field_name

        #widget_type = opts["widget"]
        # Use groupedopts widget if we specify cols, otherwise assume multiselect
        cols = opts.get("cols", None)
        if cols:
            # Grouped Checkboxes
            # @ToDo: somehow working, but ugly, not usable (deprecated?)
            if "groupedopts-filter-widget" not in _class:
                attr["_class"] = "%s groupedopts-filter-widget" % _class
            attr["cols"] = cols

            # Add one widget per level
            for level in levels:
                options = levels[level]["options"]
                groupedopts = S3GroupedOptionsWidget(cols = cols,
                                                     size = opts["size"] or 12,
                                                     )
                # Dummy field
                name = "%s-%s" % (base_name, level)
                dummy_field = Storage(name=name,
                                      type=ftype,
                                      requires=IS_IN_SET(options,
                                                         multiple=True))
                # Unique ID/name
                attr["_id"] = "%s-%s" % (base_id, level)
                attr["_name"] = name

                # Find relevant values to pre-populate
                _values = values.get("%s$%s__%s" % (fname, level, operator))
                w_append(groupedopts(dummy_field, _values, **attr))

        else:
            # Multiselect is default
            T = current.T

            # Multiselect Dropdown with Checkboxes
            if "multiselect-filter-widget" not in _class:
                _class = "%s multiselect-filter-widget" % _class

            header_opt = opts.get("header", False)
            if header_opt is False or header_opt is True:
                setting = current.deployment_settings \
                                 .get_ui_location_filter_bulk_select_option()
                if setting is not None:
                    header_opt = setting

            # Add one widget per level
            first = True
            hide = True
            s3 = current.response.s3
            for level in levels:
                # Dummy field
                name = "%s-%s" % (base_name, level)
                # Unique ID/name
                attr["_id"] = "%s-%s" % (base_id, level)
                attr["_name"] = name
                # Find relevant values to pre-populate the widget
                _values = values.get("%s$%s__%s" % (fname, level, operator))
                w = S3MultiSelectWidget(search = opts.get("search", "auto"),
                                        header = header_opt,
                                        selectedList = opts.get("selectedList", 3),
                                        noneSelectedText = T("Select %(location)s") % \
                                                             {"location": levels[level]["label"]})
                if first:
                    # Visible Multiselect Widget added to the page
                    attr["_class"] = _class
                    options = levels[level]["options"]
                    dummy_field = Storage(name = name,
                                          type = ftype,
                                          requires = IS_IN_SET(options,
                                                               multiple=True))
                    widget = w(dummy_field, _values, **attr)
                    first = False
                else:
                    # Hidden, empty dropdown added to the page, whose options and multiselect will be activated when the higher level is selected
                    if hide:
                        _class = "%s hide" % _class
                        attr["_class"] = _class
                        hide = False
                    # Store the current jquery_ready
                    jquery_ready = s3.jquery_ready
                    # Build the widget with the MultiSelect activation script
                    s3.jquery_ready = []
                    dummy_field = Storage(name = name,
                                          type = ftype,
                                          requires = IS_IN_SET([],
                                                               multiple=True))
                    widget = w(dummy_field, _values, **attr)
                    # Extract the MultiSelect activation script
                    script = s3.jquery_ready[0]
                    # Restore jquery_ready
                    s3.jquery_ready = jquery_ready
                    # Wrap the script & reinsert
                    script = '''S3.%s=function(){%s}''' % (name.replace("-", "_"), script)
                    s3.js_global.append(script)
                w_append(widget)

        # Restore id and name for the data_element
        attr["_id"] = base_id
        attr["_name"] = base_name

        # Render the filter widget
        return TAG[""](*widgets)

    # -------------------------------------------------------------------------
    def data_element(self, variable):
        """
            Construct the hidden element that holds the
            URL query term corresponding to an input element in the widget.

            Args:
                variable: the URL query variable
        """

        output = []
        oappend = output.append
        i = 0
        for level in self.levels:
            widget = INPUT(_type = "hidden",
                           _id = "%s-%s-data" % (self.attr["_id"], level),
                           _class = "filter-widget-data %s-data" % self._class,
                           _value = variable[i],
                           )
            oappend(widget)
            i += 1

        return output

    # -------------------------------------------------------------------------
    def ajax_options(self, resource):

        attr = self._attr(resource)
        levels, noopt = self._options(resource, inject_hierarchy=False)[1:3]

        opts = {}
        base_id = attr["_id"]
        for level in levels:
            if noopt:
                opts["%s-%s" % (base_id, level)] = str(noopt)
            else:
                options = levels[level]["options"]
                opts["%s-%s" % (base_id, level)] = options
        return opts

    # -------------------------------------------------------------------------
    @staticmethod
    def __options(row, levels, inject_hierarchy, hierarchy, _level, translate, name_l10n):

        if inject_hierarchy:
            parent = None
            grandparent = None
            greatgrandparent = None
            greatgreatgrandparent = None
            greatgreatgreatgrandparent = None
            i = 0
        for level in levels:
            v = row[level]
            if v:
                o = levels[level]["options"]
                if v not in o:
                    if translate:
                        o[v] = name_l10n.get(v, v)
                    else:
                        o.append(v)
                if inject_hierarchy:
                    if i == 0:
                        h = hierarchy[_level]
                        if v not in h:
                            h[v] = {}
                        parent = v
                    elif i == 1:
                        h = hierarchy[_level][parent]
                        if v not in h:
                            h[v] = {}
                        grandparent = parent
                        parent = v
                    elif i == 2:
                        h = hierarchy[_level][grandparent][parent]
                        if v not in h:
                            h[v] = {}
                        greatgrandparent = grandparent
                        grandparent = parent
                        parent = v
                    elif i == 3:
                        h = hierarchy[_level][greatgrandparent][grandparent][parent]
                        if v not in h:
                            h[v] = {}
                        greatgreatgrandparent = greatgrandparent
                        greatgrandparent = grandparent
                        grandparent = parent
                        parent = v
                    elif i == 4:
                        h = hierarchy[_level][greatgreatgrandparent][greatgrandparent][grandparent][parent]
                        if v not in h:
                            h[v] = {}
                        greatgreatgreatgrandparent = greatgreatgrandparent
                        greatgreatgrandparent = greatgrandparent
                        greatgrandparent = grandparent
                        grandparent = parent
                        parent = v
                    elif i == 5:
                        h = hierarchy[_level][greatgreatgreatgrandparent][greatgreatgrandparent][greatgrandparent][grandparent][parent]
                        if v not in h:
                            h[v] = {}
                    i += 1

    # -------------------------------------------------------------------------
    @staticmethod
    def get_lx_ancestors(levels, resource, selector=None, location_ids=None, path=False):
        """
            Look up the immediate Lx ancestors for all locations referenced
            by selector

            Args:
                levels: the relevant Lx levels, tuple of "L1", "L2" etc
                resource: the master resource
                selector: the selector for the location reference
                location_ids: use these location_ids rather than looking them
                              up from the resource
                path: include the Lx path in the result rows, to lookup
                      local names for options (which is done via IDs in
                      the path)

            Returns:
                gis_location Rows, or empty list

            Note:
                path=True potentially requires additional iterations in order
                to reduce the paths to only relevant Lx levels (so that fewer
                local names would be extracted) - which though limits the
                performance gain if there actually are only few or no translations.
                If that becomes a problem somewhere, we can make the iteration
                mode controllable by a separate parameter.
        """

        db = current.db
        s3db = current.s3db

        ltable = s3db.gis_location
        if location_ids:
            # Fixed set
            location_ids = set(location_ids)
        else:
            # Lookup from resource
            location_ids = set()

            # Resolve the selector
            rfield = resource.resolve_selector(selector)

            # Get the joins for the selector
            from ..resource import S3Joins
            joins = S3Joins(resource.tablename)
            joins.extend(rfield._joins)
            join = joins.as_list()

            # Add a join for gis_location
            join.append(ltable.on(ltable.id == rfield.field))

            # Accessible query for the master table
            query = resource.get_query()

        # Fields we want to extract for Lx ancestors
        fields = [ltable.id] + [ltable[level] for level in levels]
        if path:
            fields.append(ltable.path)

        # Suppress instantiation of LazySets in rows (we don't need them)
        rname = db._referee_name
        db._referee_name = None

        rows = []
        while True:

            if location_ids:
                query = ltable.id.belongs(location_ids)
                join = None

            # Extract all target locations resp. parents which are Lx
            if path:
                #...of relevant levels
                relevant_lx = (ltable.level.belongs(levels))
            else:
                #...of any level
                relevant_lx = (ltable.level != None)
            lx = db(query & relevant_lx).select(join = join,
                                                groupby = ltable.id,
                                                *fields
                                                )

            # Add to result rows
            if lx:
                rows = (rows | lx) if rows else lx

            # Pick subset for parent lookup
            if lx and location_ids:
                # ...all parents which are not Lx of relevant levels
                remaining = location_ids - set(row.id for row in lx)
                if remaining:
                    query = ltable.id.belongs(remaining)
                else:
                    # No more parents to look up
                    break
            else:
                # ...all locations which are not Lx
                if path:
                    # ...or not of relevant levels
                    query &= ((ltable.level == None) | (~(ltable.level.belongs(levels))))
                else:
                    query &= (ltable.level == None)

            # From subset, just extract the parent ID
            query &= (ltable.parent != None)
            parents = db(query).select(ltable.parent,
                                       join = join,
                                       groupby = ltable.parent,
                                       )

            location_ids = set(row.parent for row in parents if row.parent)
            if not location_ids:
                break

        # Restore referee name
        db._referee_name = rname

        return rows

    # -------------------------------------------------------------------------
    def _options(self, resource, inject_hierarchy=True, values=None):

        T = current.T
        s3db = current.s3db
        gtable = s3db.gis_location

        NOOPT = T("No options available")

        #attr = self.attr
        opts = self.opts
        translate = self.translate

        # Which levels should we display?
        # Lookup the appropriate labels from the GIS configuration
        if "levels" in opts:
            hierarchy = current.gis.get_location_hierarchy()
            levels = OrderedDict()
            for level in opts["levels"]:
                levels[level] = hierarchy.get(level, level)
        else:
            levels = current.gis.get_relevant_hierarchy_levels(as_dict=True)

        # Pass to data_element
        self.levels = levels

        if "label" not in opts:
            opts["label"] = T("Filter by Location")

        # Initialise Options Storage & Hierarchy
        hierarchy = {}
        first = True
        for level in levels:
            if first:
                hierarchy[level] = {}
                _level = level
                first = False
            levels[level] = {"label": levels[level],
                             "options": {} if translate else [],
                             }

        ftype = "reference gis_location"
        default = (ftype, levels, opts.get("no_opts", NOOPT))

        # Resolve the field selector
        selector = None
        if resource is None:
            rname = opts.get("resource")
            if rname:
                resource = s3db.resource(rname)
                selector = opts.get("lookup", "location_id")
        else:
            selector = self.field

        filters_added = False

        options = opts.get("options")
        if options:
            # Fixed options (=list of location IDs)
            resource = s3db.resource("gis_location", id=options)
            fields = ["id"] + [l for l in levels]
            if translate:
                fields.append("path")
            joined = False

        elif selector:

            # Lookup options from resource
            rfield = S3ResourceField(resource, selector)
            if not rfield.field or rfield.ftype != ftype:
                # Must be a real reference to gis_location
                return default

            fields = [selector] + ["%s$%s" % (selector, l) for l in levels]
            if translate:
                fields.append("%s$path" % selector)

            # Always joined (gis_location foreign key in resource)
            joined = True

            # Reduce multi-table joins by excluding empty FKs
            resource.add_filter(FS(selector) != None)

            # Filter out old Locations
            # @ToDo: Allow override
            resource.add_filter(FS("%s$end_date" % selector) == None)
            filters_added = True

        else:
            # Neither fixed options nor resource to look them up
            return default

        # Determine look-up strategy
        ancestor_lookup = opts.get("bigtable")
        if ancestor_lookup is None:
            ancestor_lookup = current.deployment_settings \
                                     .get_gis_location_filter_bigtable_lookups()

        # Find the options
        if ancestor_lookup:
            rows = self.get_lx_ancestors(levels,
                                         resource,
                                         selector = selector,
                                         location_ids = options,
                                         path = translate,
                                         )
            joined = False
        else:
            # Prevent unnecessary extraction of extra fields
            extra_fields = resource.get_config("extra_fields")
            resource.clear_config("extra_fields")

            # Suppress instantiation of LazySets in rows (we don't need them)
            db = current.db
            rname = db._referee_name
            db._referee_name = None
            rows = resource.select(fields = fields,
                                   limit = None,
                                   virtual = False,
                                   as_rows = True,
                                   )

            # Restore referee name
            db._referee_name = rname

            # Restore extra fields
            resource.configure(extra_fields=extra_fields)

        if filters_added:
            # Remove them
            rfilter = resource.rfilter
            rfilter.filters.pop()
            rfilter.filters.pop()
            rfilter.query = None
            rfilter.transformed = None

        rows2 = []
        if not rows:
            if values:
                # Make sure the selected options are in the available options

                fields = ["id"] + [l for l in levels]
                if translate:
                    fields.append("path")

                resource2 = None
                joined = False
                rows = []
                for f in values:
                    v = values[f]
                    if not v:
                        continue
                    level = "L%s" % f.split("L", 1)[1][0]
                    query = (gtable.level == level) & \
                            (gtable.name.belongs(v))
                    if resource2 is None:
                        resource2 = s3db.resource("gis_location",
                                                  filter = query,
                                                  )
                    else:
                        resource2.clear_query()
                        resource2.add_filter(query)
                    # Filter out old Locations
                    # @ToDo: Allow override
                    resource2.add_filter(gtable.end_date == None)
                    _rows = resource2.select(fields = fields,
                                             limit = None,
                                             virtual = False,
                                             as_rows = True,
                                             )
                    if rows:
                        rows &= _rows
                    else:
                        rows = _rows

            if not rows:
                # No options
                return default

        elif values:
            # Make sure the selected options are in the available options

            fields = ["id"] + [l for l in levels]
            if translate:
                fields.append("path")

            resource2 = None
            for f in values:
                v = values[f]
                if not v:
                    continue
                level = "L%s" % f.split("L", 1)[1][0]

                if resource2 is None:
                    resource2 = s3db.resource("gis_location")
                resource2.clear_query()

                query = (gtable.level == level) & \
                        (gtable.name.belongs(v))
                resource2.add_filter(query)
                # Filter out old Locations
                # @ToDo: Allow override
                resource2.add_filter(gtable.end_date == None)
                _rows = resource2.select(fields = fields,
                                         limit = None,
                                         virtual = False,
                                         as_rows = True)
                if rows2:
                    rows2 &= _rows
                else:
                    rows2 = _rows

        # Generate a name localization lookup dict
        name_l10n = {}
        if translate:
            # Get IDs via Path to lookup name_l10n
            ids = set()
            if joined:
                selector = rfield.colname
            for row in rows:
                _row = getattr(row, "gis_location") if joined else row
                path = _row.path
                if path:
                    path = path.split("/")
                else:
                    # Build it
                    if joined:
                        location_id = row[selector]
                        if location_id:
                            _row.id = location_id
                    if "id" in _row:
                        path = current.gis.update_location_tree(_row)
                        path = path.split("/")
                if path:
                    ids |= set(path)
            for row in rows2:
                path = row.path
                if path:
                    path = path.split("/")
                else:
                    # Build it
                    if "id" in row:
                        path = current.gis.update_location_tree(row)
                        path = path.split("/")
                if path:
                    ids |= set(path)

            # Build lookup table for name_l10n
            ntable = s3db.gis_location_name
            query = (gtable.id.belongs(ids)) & \
                    (ntable.deleted == False) & \
                    (ntable.location_id == gtable.id) & \
                    (ntable.language == current.session.s3.language)
            nrows = current.db(query).select(gtable.name,
                                             ntable.name_l10n,
                                             limitby = (0, len(ids)),
                                             )
            for row in nrows:
                name_l10n[row["gis_location.name"]] = row["gis_location_name.name_l10n"]

        # Populate the Options and the Hierarchy
        for row in rows:
            _row = getattr(row, "gis_location") if joined else row
            self.__options(_row, levels, inject_hierarchy, hierarchy, _level, translate, name_l10n)
        for row in rows2:
            self.__options(row, levels, inject_hierarchy, hierarchy, _level, translate, name_l10n)

        if translate:
            # Sort the options dicts
            for level in levels:
                options = levels[level]["options"]
                options = OrderedDict(sorted(options.items()))
        else:
            # Sort the options lists
            for level in levels:
                levels[level]["options"].sort()

        if inject_hierarchy:
            # Inject the Location Hierarchy
            hierarchy = "S3.location_filter_hierarchy=%s" % \
                json.dumps(hierarchy, separators=JSONSEPARATORS)
            js_global = current.response.s3.js_global
            js_global.append(hierarchy)
            if translate:
                # Inject lookup list
                name_l10n = "S3.location_name_l10n=%s" % \
                    json.dumps(name_l10n, separators=JSONSEPARATORS)
                js_global.append(name_l10n)

        return (ftype, levels, None)

    # -------------------------------------------------------------------------
    def _selector(self, resource, fields):
        """
            Helper method to generate a filter query selector for the
            given field(s) in the given resource.

            Args:
                resource: the CRUDResource
                fields: the field selectors (as strings)

            Returns:
                the field label and the filter query selector, or None if
                none of the field selectors could be resolved
        """

        prefix = self._prefix

        if resource:
            rfield = S3ResourceField(resource, fields)
            label = rfield.label
        else:
            label = None

        if "levels" in self.opts:
            levels = self.opts.levels
        else:
            levels = current.gis.get_relevant_hierarchy_levels()

        fields = ["%s$%s" % (fields, level) for level in levels]
        if resource:
            selectors = []
            for field in fields:
                try:
                    rfield = S3ResourceField(resource, field)
                except (AttributeError, TypeError):
                    continue
                selectors.append(prefix(rfield.selector))
        else:
            selectors = fields
        if selectors:
            return label, "|".join(selectors)
        else:
            return label, None

    # -------------------------------------------------------------------------
    @classmethod
    def _variable(cls, selector, operator):
        """
            Construct URL query variable(s) name from a filter query
            selector and the given operator(s)

            Args:
                selector: the selector
                operator: the operator (or tuple/list of operators)

            Returns:
                the URL query variable name (or list of variable names)
        """

        selectors = selector.split("|")
        return ["%s__%s" % (selector, operator) for selector in selectors]

# =============================================================================
class S3MapFilter(S3FilterWidget):
    """
        Map filter widget, normally configured for "~.location_id$the_geom"

        Keyword Args:
            label: label for the widget
            comment: comment for the widget
            hidden: render widget initially hidden (="advanced" option)
    """

    _class = "map-filter"

    operator = "intersects"

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        settings = current.deployment_settings

        if not settings.get_gis_spatialdb():
            current.log.warning("No Spatial DB => Cannot do Intersects Query yet => Disabling S3MapFilter")
            return ""

        attr_get = self.attr.get
        opts_get = self.opts.get

        _class = attr_get("class")
        if _class:
            _class = "%s %s" % (_class, self._class)
        else:
            _class = self._class

        _id = attr_get("_id")

        # Hidden INPUT to store the WKT
        hidden_input = INPUT(_type = "hidden",
                             _class = _class,
                             _id = _id,
                             )

        # Populate with the value, if given
        if values not in (None, []):
            if type(values) is list:
                values = values[0]
            hidden_input["_value"] = values

        # Map Widget
        map_id = "%s-map" % _id

        c, f = resource.tablename.split("_", 1)
        c = opts_get("controller", c)
        f = opts_get("function", f)

        ltable = current.s3db.gis_layer_feature
        query = (ltable.controller == c) & \
                (ltable.function == f) & \
                (ltable.deleted == False)
        layer = current.db(query).select(ltable.layer_id,
                                         ltable.name,
                                         limitby=(0, 1)
                                         ).first()
        try:
            layer_id = layer.layer_id
        except AttributeError:
            # No prepop done?
            layer_id = None
            layer_name = resource.tablename
        else:
            layer_name = layer.name

        feature_resources = [{"name"     : current.T(layer_name),
                              "id"       : "search_results",
                              "layer_id" : layer_id,
                              "filter"   : opts_get("filter"),
                              },
                             ]

        button = opts_get("button")
        if button:
            # No need for the toolbar
            toolbar = opts_get("toolbar", False)
        else:
            # Need the toolbar
            toolbar = True

        _map = current.gis.show_map(id = map_id,
                                    height = opts_get("height", settings.get_gis_map_height()),
                                    width = opts_get("width", settings.get_gis_map_width()),
                                    collapsed = True,
                                    callback = '''S3.search.s3map('%s')''' % map_id,
                                    feature_resources = feature_resources,
                                    toolbar = toolbar,
                                    add_polygon = True,
                                    )

        return TAG[""](hidden_input,
                       button,
                       _map,
                       )

# =============================================================================
class S3OptionsFilter(S3FilterWidget):
    """
        Options filter widget

        Keyword Args:
            ** Widget appearance:
            label: label for the widget
            comment: comment for the widget
            hidden: render widget initially hidden (="advanced" option)
            widget: widget to use, "select", "multiselect" (default),
                    or "groupedopts"
            no_opts: text to show if no options available

            ** Options-lookup:
            resource: alternative resource to look up options
            lookup: field in the alternative resource to look up
            options: fixed set of options (of {value: label} or a callable
                     that returns one)

            ** Options-representation:
            represent: custom represent for looked-up options
                       (overrides field representation method)
            translate: translate the option labels in the fixed set (looked-up
                       option sets will use the field representation method
                       instead)
            none: label for explicit None-option in many-to-many fields

            ** multiselect-specific options:
            search: show search-field to search for options
            header: show header with bulk-actions
            selectedList: number of selected items to show on button before
                          collapsing into number of items

            ** groupedopts-specific options:
            cols: number of columns of checkboxes
            size: maximum size of multi-letter options groups
            help_field: field in the referenced table to display on hovering
                        over a foreign key option

            ** special purpose / custom filters:
            anyall: use user-selectable any/all alternatives even if field is
                    not a list-type
    """

    _class = "options-filter"

    operator = "belongs"

    alternatives = ["anyof", "contains"]

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        attr = self._attr(resource)
        opts_get = self.opts.get
        name = attr["_name"]

        # Get the options
        ftype, options, noopt = self._options(resource, values=values)
        if options is None:
            options = []
            hide_widget = True
            hide_noopt = ""
        else:
            options = OrderedDict(options)
            hide_widget = False
            hide_noopt = " hide"

        # Any-All-Option : for many-to-many fields the user can
        # search for records containing all the options or any
        # of the options:
        if len(options) > 1 and (ftype[:4] == "list" or opts_get("anyall")):
            operator = opts_get("operator", None)
            if operator:
                # Fixed operator
                any_all = ""
            else:
                # User choice (initially set to "all")
                any_all = True
                operator = "contains"

            if operator == "anyof":
                filter_type = "any"
            else:
                filter_type = "all"
            self.operator = operator

            if any_all:
                # Provide a form to prompt the user to choose
                T = current.T
                any_all = DIV(LABEL("%s:" % T("Match")),
                              LABEL(INPUT(_name = "%s_filter" % name,
                                          _id = "%s_filter_any" % name,
                                          _type = "radio",
                                          _value = "any",
                                          value = filter_type,
                                          ),
                                    T("Any##filter_options"),
                                    _for = "%s_filter_any" % name,
                                    ),
                              LABEL(INPUT(_name = "%s_filter" % name,
                                          _id = "%s_filter_all" % name,
                                          _type = "radio",
                                          _value = "all",
                                          value = filter_type,
                                          ),
                                    T("All##filter_options"),
                                    _for = "%s_filter_all" % name,
                                    ),
                              _class="s3-options-filter-anyall",
                              )
        else:
            any_all = ""

        # Initialize widget
        #widget_type = opts_get("widget")
        # Use groupedopts widget if we specify cols, otherwise assume multiselect
        cols = opts_get("cols", None)
        if cols:
            widget_class = "groupedopts-filter-widget"
            w = S3GroupedOptionsWidget(options = options,
                                       multiple = opts_get("multiple", True),
                                       cols = cols,
                                       size = opts_get("size", 12),
                                       help_field = opts_get("help_field"),
                                       sort = opts_get("sort", True),
                                       orientation = opts_get("orientation"),
                                       table = opts_get("table", True),
                                       no_opts = opts_get("no_opts", None),
                                       option_comment = opts_get("option_comment", False),
                                       )
        else:
            # Default widget_type = "multiselect"
            widget_class = "multiselect-filter-widget"
            w = S3MultiSelectWidget(search = opts_get("search", "auto"),
                                    header = opts_get("header", False),
                                    selectedList = opts_get("selectedList", 3),
                                    noneSelectedText = opts_get("noneSelectedText", "Select"),
                                    multiple = opts_get("multiple", True),
                                    )


        # Add widget class and default class
        classes = attr.get("_class", "").split() + [widget_class, self._class]
        if hide_widget:
            classes.append("hide")
        attr["_class"] = " ".join(set(classes)) if classes else None

        # Render the widget
        dummy_field = Storage(name = name,
                              type = ftype,
                              requires = IS_IN_SET(options, multiple=True),
                              )
        widget = w(dummy_field, values, **attr)

        return TAG[""](any_all,
                       widget,
                       SPAN(noopt,
                            _class = "no-options-available%s" % hide_noopt,
                            ),
                       )

    # -------------------------------------------------------------------------
    def ajax_options(self, resource):
        """
            Method to Ajax-retrieve the current options of this widget

            Args:
                resource: the CRUDResource
        """

        opts = self.opts
        attr = self._attr(resource)
        ftype, options, noopt = self._options(resource)

        if options is None:
            options = {attr["_id"]: {"empty": str(noopt)}}
        else:
            #widget_type = opts["widget"]
            # Use groupedopts widget if we specify cols, otherwise assume multiselect
            cols = opts.get("cols", None)
            if cols:
                # Use the widget method to group and sort the options
                widget = S3GroupedOptionsWidget(
                                options = options,
                                multiple = True,
                                cols = cols,
                                size = opts["size"] or 12,
                                help_field = opts["help_field"],
                                sort = opts.get("sort", True),
                                )
                options = {attr["_id"]:
                           widget._options({"type": ftype}, [])}
            else:
                # Multiselect
                # Produce a simple list of tuples
                options = {attr["_id"]: [(k, s3_str(v))
                                         for k, v in options]}

        return options

    # -------------------------------------------------------------------------
    def _options(self, resource, values=None):
        """
            Helper function to retrieve the current options for this
            filter widget

            Args:
                resource: the CRUDResource
        """

        T = current.T
        NOOPT = T("No options available")
        EMPTY = T("None")

        #attr = self.attr
        opts = self.opts

        # Resolve the field selector
        selector = self.field
        if isinstance(selector, (tuple, list)):
            selector = selector[0]

        if resource is None:
            rname = opts.get("resource")
            if rname:
                resource = current.s3db.resource(rname)

        if resource:
            rfield = S3ResourceField(resource, selector)
            field = rfield.field
            colname = rfield.colname
            ftype = rfield.ftype
        else:
            rfield = field = colname = None
            ftype = "string"

        # Find the options
        opt_keys = []

        multiple = ftype[:5] == "list:"
        if opts.options is not None:
            # Custom dict of options {value: label} or a callable
            # returning such a dict:
            options = opts.options
            if callable(options):
                options = options()
            opt_keys = list(options.keys())

        elif resource:
            # Determine the options from the field type
            options = None
            if ftype == "boolean":
                opt_keys = (True, False)

            elif field or rfield.virtual:

                groupby = field if field and not multiple else None
                virtual = field is None

                # If the search field is a foreign key, then try to perform
                # a reverse lookup of primary IDs in the lookup table which
                # are linked to at least one record in the resource => better
                # scalability
                # => only if the number of lookup options is much (!) smaller than
                #    the number of records in the resource to filter, otherwise
                #    this can have the opposite effect (e.g. person_id being the
                #    search field); however, counting records in both tables before
                #    deciding this would be even less scalable, hence:
                # @todo: implement a widget option to enforce forward-lookup if
                #        the look-up table is the big table
                rows = None
                if field:
                    ktablename, key, m = s3_get_foreign_key(field, m2m=False)
                    if ktablename:

                        multiple = m

                        ktable = current.s3db.table(ktablename)
                        key_field = ktable[key]
                        colname = str(key_field)

                        # Find only values linked to records the user is
                        # permitted to read, and apply any resource filters
                        # (= use the resource query)
                        query = resource.get_query()

                        # Must include rfilter joins when using the resource
                        # query (both inner and left):
                        rfilter = resource.rfilter
                        if rfilter:
                            join = rfilter.get_joins()
                            left = rfilter.get_joins(left=True)
                        else:
                            join = left = None

                        # The actual query for the look-up table
                        # NB the inner join here is required even if rfilter
                        #    already left-joins the look-up table, because we
                        #    must make sure look-up values are indeed linked
                        #    to the resource => not redundant!
                        query &= (key_field == field) & \
                                 current.auth.s3_accessible_query("read", ktable)

                        # Exclude deleted keys
                        # => there should be no references to deleted keys, so
                        #    they are already excluded by (key_field == field),
                        #    hence this is redundant:
                        #if "deleted" in ktable.fields:
                        #    query &= (ktable.deleted == False)

                        # If the filter field is in a joined table itself,
                        # then we also need the join for that table (this
                        # could be redundant, but checking that will likely
                        # take more effort than we can save by avoiding it)
                        joins = rfield.join
                        for tname in joins:
                            query &= joins[tname]

                        # Filter options by location?
                        location_filter = opts.get("location_filter")
                        if location_filter and "location_id" in ktable:
                            location = current.session.s3.location_filter
                            if location:
                                query &= (ktable.location_id == location)

                        # Filter options by organisation?
                        org_filter = opts.get("org_filter")
                        if org_filter and "organisation_id" in ktable:
                            root_org = current.auth.root_org()
                            if root_org:
                                query &= ((ktable.organisation_id == root_org) | \
                                          (ktable.organisation_id == None))
                            #else:
                            #    query &= (ktable.organisation_id == None)

                        rows = current.db(query).select(key_field,
                                                        resource._id.min(),
                                                        groupby = key_field,
                                                        join = join,
                                                        left = left,
                                                        )

                # If we can not perform a reverse lookup, then we need
                # to do a forward lookup of all unique values of the
                # search field from all records in the table :/ still ok,
                # but not endlessly scalable:
                if rows is None:
                    rows = resource.select([selector],
                                           limit = None,
                                           orderby = field,
                                           groupby = groupby,
                                           virtual = virtual,
                                           as_rows = True,
                                           )

                opt_keys = [] # Can't use set => would make orderby pointless
                if rows:
                    kappend = opt_keys.append
                    kextend = opt_keys.extend
                    for row in rows:
                        val = row[colname]
                        if virtual and callable(val):
                            val = val()
                        if (multiple or \
                            virtual) and isinstance(val, (list, tuple, set)):
                            kextend([v for v in val
                                       if v not in opt_keys])
                        elif val not in opt_keys:
                            kappend(val)

        # Make sure the selected options are in the available options
        # (not possible if we have a fixed options dict)
        if options is None and values:
            numeric = rfield.ftype in ("integer", "id") or \
                      rfield.ftype[:9] == "reference"
            for _val in values:
                if numeric and _val is not None:
                    try:
                        val = int(_val)
                    except ValueError:
                        # not valid for this field type => skip
                        continue
                else:
                    val = _val
                if val not in opt_keys and \
                   (not isinstance(val, int) or not str(val) in opt_keys):
                    opt_keys.append(val)

        # No options?
        if len(opt_keys) < 1 or len(opt_keys) == 1 and not opt_keys[0]:
            return (ftype, None, opts.get("no_opts", NOOPT))

        # Represent the options
        opt_list = [] # list of tuples (key, value)

        # Custom represent? (otherwise fall back to field.represent)
        represent = opts.represent
        if not represent: # or ftype[:9] != "reference":
            represent = field.represent if field else None

        if options is not None:
            # Custom dict of {value:label} => use this label
            if opts.get("translate"):
                # Translate the labels
                opt_list = [(opt, T(label))
                            if isinstance(label, str) else (opt, label)
                            for opt, label in options.items()
                            ]
            else:
                opt_list = list(options.items())


        elif callable(represent):
            # Callable representation function:

            if hasattr(represent, "bulk"):
                # S3Represent => use bulk option
                opt_dict = represent.bulk(opt_keys,
                                          list_type=False,
                                          show_link=False)
                if None in opt_keys:
                    opt_dict[None] = EMPTY
                elif None in opt_dict:
                    del opt_dict[None]
                if "" in opt_keys:
                    opt_dict[""] = EMPTY
                opt_list = list(opt_dict.items())

            else:
                # Simple represent function
                varnames = represent.__code__.co_varnames
                args = {"show_link": False} if "show_link" in varnames else {}
                if multiple:
                    repr_opt = lambda opt: opt in (None, "") and (opt, EMPTY) or \
                                           (opt, represent([opt], **args))
                else:
                    repr_opt = lambda opt: opt in (None, "") and (opt, EMPTY) or \
                                           (opt, represent(opt, **args))
                opt_list = [repr_opt(k) for k in opt_keys]

        elif isinstance(represent, str) and ftype[:9] == "reference":
            # Represent is a string template to be fed from the
            # referenced record

            # Get the referenced table
            db = current.db
            ktable = db[ftype[10:]]

            k_id = ktable._id.name

            # Get the fields referenced by the string template
            fieldnames = [k_id]
            fieldnames += re.findall(r"%\(([a-zA-Z0-9_]*)\)s", represent)
            represent_fields = [ktable[fieldname] for fieldname in fieldnames]

            # Get the referenced records
            query = (ktable.id.belongs([k for k in opt_keys
                                              if str(k).isdigit()])) & \
                    (ktable.deleted == False)
            rows = db(query).select(*represent_fields).as_dict(key=k_id)

            # Run all referenced records against the format string
            opt_list = []
            ol_append = opt_list.append
            for opt_value in opt_keys:
                if opt_value in rows:
                    opt_represent = represent % rows[opt_value]
                    if opt_represent:
                        ol_append((opt_value, opt_represent))

        else:
            # Straight string representations of the values (fallback)
            opt_list = [(opt_value, s3_str(opt_value))
                        for opt_value in opt_keys if opt_value]

        if opts.get("sort", True):
            try:
                opt_list.sort(key=lambda item: item[1])
            except:
                opt_list.sort(key=lambda item: s3_str(item[1]))
        options = []
        empty = False
        none = opts["none"]
        for k, v in opt_list:
            if k is None:
                if none:
                    empty = True
                    if none is True:
                        # Use the represent
                        options.append((k, v))
                    else:
                        # Must be a string to use as the represent:
                        options.append((k, none))
            else:
                options.append((k, v))
        if none and not empty:
            # Add the value anyway (e.g. not found via the reverse lookup)
            if none is True:
                none = current.messages["NONE"]
            options.append((None, none))

        if not opts.get("multiple", True) and not self.values:
            # Browsers automatically select the first option in single-selects,
            # but that doesn't filter the data, so the first option must be
            # empty if we don't have a default:
            options.insert(0, ("", "")) # XML("&nbsp;") better?

        # Sort the options
        return (ftype, options, opts.get("no_opts", NOOPT))

    # -------------------------------------------------------------------------
    @staticmethod
    def _values(get_vars, variable):
        """
            Helper method to get all values of a URL query variable

            Args:
                get_vars: the GET vars (a dict)
                variable: the name of the query variable

            Returns:
                a list of values
        """

        if not variable:
            return []

        # Match __eq before checking any other operator
        selector = S3URLQuery.parse_key(variable)[0]
        for key in ("%s__eq" % selector, selector, variable):
            if key in get_vars:
                values = S3URLQuery.parse_value(get_vars[key])
                if not isinstance(values, (list, tuple)):
                    values = [values]
                return values

        return []

# =============================================================================
class S3HierarchyFilter(S3FilterWidget):
    """
        Filter widget for hierarchical types

        Keyword Arguments:
            lookup: name of the lookup table
            represent: representation method for the key
            multiple: allow selection of multiple options
            leafonly: only leaf nodes can be selected
            cascade: automatically select child nodes when selecting a
                     parent node
            bulk_select: provide an option to select/deselect all nodes

        See Also:
            S3HierarchyWidget
    """

    _class = "hierarchy-filter"

    operator = "belongs"

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        # Currently selected values
        selected = []
        append = selected.append
        if not isinstance(values, (list, tuple, set)):
            values = [values]
        for v in values:
            if isinstance(v, int) or str(v).isdigit():
                append(v)

        # Resolve the field selector
        rfield = S3ResourceField(resource, self.field)

        # Instantiate the widget
        opts = self.opts
        bulk_select = current.deployment_settings \
                             .get_ui_hierarchy_filter_bulk_select_option()
        if bulk_select is None:
            bulk_select = opts.get("bulk_select", False)

        if opts.get("widget") == "cascade":
            formstyle = current.deployment_settings.get_ui_filter_formstyle()
            w = S3CascadeSelectWidget(lookup = opts.get("lookup"),
                                      formstyle = formstyle,
                                      multiple = opts.get("multiple", True),
                                      filter = opts.get("filter"),
                                      leafonly = opts.get("leafonly", True),
                                      cascade = opts.get("cascade"),
                                      represent = opts.get("represent"),
                                      inline = True,
                                      )
        else:
            w = S3HierarchyWidget(lookup = opts.get("lookup"),
                                  multiple = opts.get("multiple", True),
                                  filter = opts.get("filter"),
                                  leafonly = opts.get("leafonly", True),
                                  cascade = opts.get("cascade", False),
                                  represent = opts.get("represent"),
                                  bulk_select = bulk_select,
                                  none = opts.get("none"),
                                  )

        # Render the widget
        widget = w(rfield.field, selected, **self._attr(resource))
        widget.add_class(self._class)

        return widget

    # -------------------------------------------------------------------------
    def variable(self, resource, get_vars=None):
        """
            Generate the name for the URL query variable for this
            widget, detect alternative __typeof queries.

            Args:
                resource: the resource

            Returns:
                the URL query variable name (or list of variable names if
                there are multiple operators)
        """

        label, self.selector = self._selector(resource, self.field)

        if not self.selector:
            return None

        if "label" not in self.opts:
            self.opts["label"] = label

        selector = self.selector

        if self.alternatives and get_vars is not None:
            # Get the actual operator from get_vars
            operator = self._operator(get_vars, self.selector)
            if operator:
                self.operator = operator

        variable = self._variable(selector, self.operator)

        if not get_vars or not resource or variable in get_vars:
            return variable

        # Detect and resolve __typeof queries
        resolve = S3ResourceQuery._resolve_hierarchy
        selector = resource.prefix_selector(selector)
        for key, value in list(get_vars.items()):

            if key.startswith(selector):
                selectors, op = S3URLQuery.parse_expression(key)[:2]
            else:
                continue
            if op != "typeof" or len(selectors) != 1:
                continue

            rfield = resource.resolve_selector(selectors[0])
            if rfield.field:
                values = S3URLQuery.parse_value(value)
                field, nodeset, none = resolve(rfield.field, values)[1:]
                if field and (nodeset or none):
                    if nodeset is None:
                        nodeset = set()
                    if none:
                        nodeset.add(None)
                    get_vars.pop(key, None)
                    get_vars[variable] = [str(v) for v in nodeset]
            break

        return variable

# =============================================================================
class S3NotEmptyFilter(S3FilterWidget):
    """
        Filter to check for presence of any (non-None) value in a field
    """

    _class = "value-filter"

    operator = "ne"

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        attr = self.attr
        _class = self._class
        if "_class" in attr and attr["_class"]:
            _class = "%s %s" % (attr["_class"], _class)
        else:
            _class = _class
        attr["_class"] = _class
        attr["_type"] = "checkbox"
        attr["value"] = True if "None" in values else False

        return INPUT(**attr)

# =============================================================================
class S3EmptyFilter(S3FilterWidget):
    """
        Filter to check for No Component records of a type
        - e.g. Filter out all those people who have a certain qualification already
        - use with a Custom Filtered Component "custom.id"
    """

    _class = "value-filter"

    operator = "eq"

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        attr = self.attr
        _class = self._class
        if "_class" in attr and attr["_class"]:
            _class = "%s %s" % (attr["_class"], _class)
        else:
            _class = _class
        attr["_class"] = _class
        attr["_type"] = "checkbox"
        attr["value"] = True if None in values else False

        return INPUT(**attr)

# =============================================================================
class S3FilterForm:
    """ Helper class to construct and render a filter form for a resource """

    def __init__(self, widgets, **attr):
        """
            Constructor

            Args:
                widgets: the widgets (as list)
                attr: HTML attributes for this form
        """

        self.widgets = widgets

        attributes = Storage()
        options = Storage()
        for k, v in attr.items():
            if k[0] == "_":
                attributes[k] = v
            else:
                options[k] = v
        self.attr = attributes
        self.opts = options

    # -------------------------------------------------------------------------
    def html(self, resource, get_vars=None, target=None, alias=None):
        """
            Render this filter form as HTML form.

            Args:
                resource: the CRUDResource
                get_vars: the request GET vars (URL query dict)
                target: the HTML element ID of the target object for
                        this filter form (e.g. a datatable)
                alias: the resource alias to use in widgets
        """

        attr = self.attr
        form_id = attr.get("_id")
        if not form_id:
            form_id = "filter-form"
        attr["_id"] = form_id

        # Prevent issues with Webkit-based browsers & Back buttons
        attr["_autocomplete"] = "off"

        opts_get = self.opts.get
        settings = current.deployment_settings

        # Form style
        formstyle = opts_get("formstyle", None)
        if not formstyle:
            formstyle = settings.get_ui_filter_formstyle()

        # Filter widgets
        rows = self._render_widgets(resource,
                                    get_vars = get_vars or {},
                                    alias = alias,
                                    formstyle = formstyle,
                                    )

        # Filter Manager (load/apply/save filters)
        fm = settings.get_search_filter_manager()
        if fm and opts_get("filter_manager", resource is not None):
            filter_manager = self._render_filters(resource, form_id)
        else:
            filter_manager = None

        # Other filter form controls
        controls = self._render_controls(resource, filter_manager)
        if controls:
            rows.append(formstyle(None, "", controls, ""))

        # Submit elements
        ajax = opts_get("ajax", False)
        submit = opts_get("submit", False)
        if submit:
            # Auto-submit?
            auto_submit = settings.get_ui_filter_auto_submit()
            if auto_submit and opts_get("auto_submit", True):
                script = '''S3.search.filterFormAutoSubmit('%s',%s)''' % \
                         (form_id, auto_submit)
                current.response.s3.jquery_ready.append(script)

            # Custom label and class
            _class = None
            if submit is True:
                label = current.T("Search")
            elif isinstance(submit, (list, tuple)):
                label, _class = submit
            else:
                label = submit

            # Submit button
            submit_button = INPUT(_type = "button",
                                  _value = label,
                                  _class = "filter-submit",
                                  )
            if _class:
                submit_button.add_class(_class)

            # Where to request filtered data from:
            submit_url = opts_get("url", URL(vars={}))

            # Where to request updated options from:
            ajax_url = opts_get("ajaxurl", URL(args=["filter.options"], vars={}))

            # Submit row elements
            submit = TAG[""](submit_button,
                             INPUT(_type = "hidden",
                                   _class = "filter-ajax-url",
                                   _value = ajax_url,
                                   ),
                             INPUT(_type = "hidden",
                                   _class = "filter-submit-url",
                                   _value = submit_url,
                                   ))
            if ajax and target:
                submit.append(INPUT(_type = "hidden",
                                    _class = "filter-submit-target",
                                    _value = target,
                                    ))

            # Append submit row
            submit_row = formstyle(None, "", submit, "")
            if auto_submit and hasattr(submit_row, "add_class"):
                submit_row.add_class("hide")
            rows.append(submit_row)

        # Filter Manager (load/apply/save filters)
        if filter_manager:
            fmrow = formstyle(None, "", filter_manager, "")
            if hasattr(fmrow, "add_class"):
                fmrow.add_class("hide filter-manager-row")
            rows.append(fmrow)

        # Adapt to formstyle: render a TABLE only if formstyle returns TRs
        if rows:
            elements = rows[0]
            if not isinstance(elements, (list, tuple)):
                elements = elements.elements()
            n = len(elements)
            if n > 0 and elements[0].tag == "tr" or \
               n > 1 and elements[0].tag == "" and elements[1].tag == "tr":
                form = FORM(TABLE(TBODY(rows)), **attr)
            else:
                form = FORM(DIV(rows), **attr)
                if settings.ui.formstyle == "bootstrap":
                    # We need to amend the HTML markup to support this CSS framework
                    form.add_class("form-horizontal")
            form.add_class("filter-form")
            if ajax:
                form.add_class("filter-ajax")
        else:
            return ""

        # Put a copy of formstyle into the form for access by the view
        form.formstyle = formstyle
        return form

    # -------------------------------------------------------------------------
    def fields(self, resource, get_vars=None, alias=None):
        """
            Render the filter widgets without FORM wrapper, e.g. to
            embed them as fieldset in another form.

            Args:
                resource: the CRUDResource
                get_vars: the request GET vars (URL query dict)
                alias: the resource alias to use in widgets
        """

        formstyle = self.opts.get("formstyle", None)
        if not formstyle:
            formstyle = current.deployment_settings.get_ui_filter_formstyle()

        rows = self._render_widgets(resource,
                                    get_vars = get_vars,
                                    alias = alias,
                                    formstyle = formstyle)

        controls = self._render_controls(resource)
        if controls:
            rows.append(formstyle(None, "", controls, ""))

        # Adapt to formstyle: only render a TABLE if formstyle returns TRs
        if rows:
            elements = rows[0]
            if not isinstance(elements, (list, tuple)):
                elements = elements.elements()
            n = len(elements)
            if n > 0 and elements[0].tag == "tr" or \
               n > 1 and elements[0].tag == "" and elements[1].tag == "tr":
                fields = TABLE(TBODY(rows))
            else:
                fields = DIV(rows)

        return fields

    # -------------------------------------------------------------------------
    def _render_controls(self, resource, filter_manager=None):
        """
            Render optional additional filter form controls: advanced
            options toggle, clear filters.

            Args:
                resource: the resource
                filter_manager: the filter manager widget
        """

        T = current.T
        controls = []
        opts = self.opts

        advanced = opts.get("advanced", False)
        if advanced:
            _class = "filter-advanced"
            if advanced is True:
                label = T("More Options")
            elif isinstance(advanced, (list, tuple)):
                label = advanced[0]
                label = advanced[1]
                if len(advanced > 2):
                    _class = "%s %s" % (advanced[2], _class)
            else:
                label = advanced
            label_off = T("Less Options")
            advanced = A(SPAN(label,
                              data = {"on": label,
                                      "off": label_off,
                                      },
                              _class = "filter-advanced-label",
                              ),
                         ICON("down"),
                         ICON("up", _style = "display:none"),
                         _class=_class
                         )
            controls.append(advanced)

        clear = opts.get("clear", True)
        if clear:
            _class = "filter-clear"
            if clear is True:
                label = T("Clear Filter")
            elif isinstance(clear, (list, tuple)):
                label = clear[0]
                _class = "%s %s" % (clear[1], _class)
            else:
                label = clear
            clear = A(label, _class=_class)
            clear.add_class("action-lnk")
            controls.append(clear)

        if filter_manager:
            show_fm = A(T("Saved Filters"),
                        _class = "show-filter-manager action-lnk",
                        )
            controls.append(show_fm)

        return DIV(controls, _class="filter-controls") if controls else None

    # -------------------------------------------------------------------------
    def _render_widgets(self,
                        resource,
                        get_vars=None,
                        alias=None,
                        formstyle=None):
        """
            Render the filter widgets

            Args:
                resource: the CRUDResource
                get_vars: the request GET vars (URL query dict)
                alias: the resource alias to use in widgets
                formstyle: the formstyle to use

            Returns:
                a list of form rows
        """

        rows = []
        rappend = rows.append
        advanced = False
        for f in self.widgets:
            if not f:
                continue
            widget = f(resource, get_vars, alias=alias)
            widget_opts = f.opts
            label = widget_opts["label"]
            comment = widget_opts["comment"]
            hidden = widget_opts["hidden"]
            widget_formstyle = widget_opts.get("formstyle", formstyle)
            if hidden:
                advanced = True
            widget_id = f.attr["_id"]
            if widget_id:
                row_id = "%s__row" % widget_id
                label_id = "%s__label" % widget_id
            else:
                row_id = None
                label_id = None
            if label:
                label = LABEL("%s:" % label, _id=label_id, _for=widget_id)
            elif label is not False:
                label = ""
            if not comment:
                comment = ""
            formrow = widget_formstyle(row_id, label, widget, comment, hidden=hidden)
            if hidden:
                if isinstance(formrow, DIV):
                    formrow.add_class("advanced")
                elif isinstance(formrow, tuple):
                    for item in formrow:
                        if hasattr(item, "add_class"):
                            item.add_class("advanced")
            rappend(formrow)
        if advanced:
            if resource:
                self.opts["advanced"] = \
                    resource.get_config("filter_advanced", True)
            else:
                self.opts["advanced"] = True
        return rows

    # -------------------------------------------------------------------------
    def _render_filters(self, resource, form_id):
        """
            Render a filter manager widget

            Args:
                resource: the resource

            Returns:
                the widget
        """

        SELECT_FILTER = current.T("Saved Filters")

        ajaxurl = self.opts.get("saveurl", URL(args=["filter.json"], vars={}))

        # Current user
        auth = current.auth
        pe_id = auth.user.pe_id if auth.s3_logged_in() else None
        if not pe_id:
            return None

        table = current.s3db.pr_filter
        query = (table.deleted == False) & \
                (table.pe_id == pe_id)

        if resource:
            query &= (table.resource == resource.tablename)
        else:
            query &= (table.resource == None)

        rows = current.db(query).select(table._id,
                                        table.title,
                                        table.query,
                                        orderby = table.title
                                        )

        options = [OPTION(SELECT_FILTER,
                          _value = "",
                          _class = "filter-manager-prompt",
                          _disabled = "disabled",
                          )]
        add_option = options.append
        filters = {}
        for row in rows:
            filter_id = row[table._id]
            add_option(OPTION(row.title, _value=filter_id))
            query = row.query
            if query:
                query = json.loads(query)
            filters[filter_id] = query
        widget_id = "%s-fm" % form_id
        widget = DIV(SELECT(options,
                            _id = widget_id,
                            _class = "filter-manager-widget",
                            ),
                     _class = "filter-manager-container",
                     )

        # JSON-serializable translator
        T = current.T
        t_ = lambda s: s3_str(T(s))

        # Configure the widget
        settings = current.deployment_settings
        config = {# Filters and Ajax URL
                  "filters": filters,
                  "ajaxURL": ajaxurl,

                  # Workflow Options
                  "allowDelete": settings.get_search_filter_manager_allow_delete(),

                  # Tooltips for action icons/buttons
                  "createTooltip": t_("Save current options as new filter"),
                  "loadTooltip": t_("Load filter"),
                  "saveTooltip": t_("Update saved filter"),
                  "deleteTooltip": t_("Delete saved filter"),

                  # Hints
                  "titleHint": t_("Enter a title..."),
                  "selectHint": s3_str(SELECT_FILTER),
                  "emptyHint": t_("No saved filters"),

                  # Confirm update + confirmation text
                  "confirmUpdate": t_("Update this filter?"),
                  "confirmDelete": t_("Delete this filter?"),
                  }

        # Render actions as buttons with text if configured, otherwise
        # they will appear as empty DIVs with classes for CSS icons
        create_text = settings.get_search_filter_manager_save()
        if create_text:
            config["createText"] = t_(create_text)
        update_text = settings.get_search_filter_manager_update()
        if update_text:
            config["saveText"] = t_(update_text)
        delete_text = settings.get_search_filter_manager_delete()
        if delete_text:
            config["deleteText"] = t_(delete_text)
        load_text = settings.get_search_filter_manager_load()
        if load_text:
            config["loadText"] = t_(load_text)

        script = '''$("#%s").filtermanager(%s)''' % \
                    (widget_id,
                     json.dumps(config, separators=JSONSEPARATORS))

        current.response.s3.jquery_ready.append(script)

        return widget

    # -------------------------------------------------------------------------
    def json(self, resource, get_vars=None):
        """
            Render this filter form as JSON (for Ajax requests)

            Args:
                resource: the CRUDResource
                get_vars: the request GET vars (URL query dict)
        """

        raise NotImplementedError

    # -------------------------------------------------------------------------
    @staticmethod
    def apply_filter_defaults(request, resource):
        """
            Add default filters to resource, to be called on a multi-record
            view when a filter form is rendered the first time and before
            the view elements get processed; can be overridden in request
            URL with ?default_filters=0

            Args:
                request: the request
                resource: the resource

            Returns:
                dict with default filters (URL vars)
        """

        default_filters = {}

        get_vars = request.get_vars
        if get_vars.get("default_filters") == "0":
            # Skip default filters (e.g. link in report)
            return default_filters

        s3 = current.response.s3
        tablename = resource.tablename

        # Do we have filter defaults for this resource?
        filter_defaults = s3
        for level in ("filter_defaults", tablename):
            if level not in filter_defaults:
                filter_defaults = None
                break
            filter_defaults = filter_defaults[level]

        # Which filter widgets do we need to apply defaults for?
        filter_widgets = resource.get_config("filter_widgets")
        for filter_widget in filter_widgets:

            # Do not apply defaults of hidden widgets because they are
            # not visible to the user:
            if not filter_widget or filter_widget.opts.hidden:
                continue

            has_default = False
            if "default" in filter_widget.opts:
                has_default = True
            elif filter_defaults is None:
                continue

            # Use alias in selectors if looking at a component
            filter_widget.alias = resource.alias if resource.parent else None

            defaults = set()
            variable = filter_widget.variable(resource, get_vars)
            multiple = type(variable) is list

            # Do we have a corresponding value in get_vars?
            if multiple:
                for k in variable:
                    values = filter_widget._values(get_vars, k)
                    if values:
                        filter_widget.values[k] = values
                    else:
                        defaults.add(k)
            else:
                values = filter_widget._values(get_vars, variable)
                if values:
                    filter_widget.values[variable] = values
                else:
                    defaults.add(variable)

            # Extract widget default
            if has_default:
                widget_default = filter_widget.opts["default"]
                if not isinstance(widget_default, dict):
                    if multiple:
                        widget_default = dict((k, widget_default)
                                              for k in variable)
                    else:
                        widget_default = {variable: widget_default}
                for k in widget_default:
                    if k not in filter_widget.values:
                        defaults.add(k)
            else:
                widget_default = {}

            for variable in defaults:
                selector, operator, invert = S3URLQuery.parse_key(variable)
                if invert:
                    operator = "%s!" % operator

                if filter_defaults and selector in filter_defaults:
                    applicable_defaults = filter_defaults[selector]
                elif variable in widget_default:
                    applicable_defaults = widget_default[variable]
                else:
                    continue

                if callable(applicable_defaults):
                    applicable_defaults = applicable_defaults(selector,
                                                              tablename = tablename)
                if isinstance(applicable_defaults, dict):
                    if operator in applicable_defaults:
                        default = applicable_defaults[operator]
                    else:
                        continue
                elif operator in (None, "belongs", "eq", "ne", "like"):
                    default = applicable_defaults
                else:
                    continue
                if default is None:
                    # Ignore (return [None] to filter for None)
                    continue
                elif not isinstance(default, list):
                    default = [default]
                filter_widget.values[variable] = [str(v) if v is None else v
                                                  for v in default]
                default_filters[variable] = ",".join(s3_str(v)
                                                     for v in default)

            # Apply to resource
            queries = S3URLQuery.parse(resource, default_filters)
            add_filter = resource.add_filter
            for alias in queries:
                for q in queries[alias]:
                    add_filter(q)

        return default_filters

# =============================================================================
class S3FilterString:
    """
        Helper class to render a human-readable representation of a
        filter query, as representation method of JSON-serialized
        queries in saved filters.
    """

    def __init__(self, resource, query):
        """
            Constructor

            Args:
                query: the URL query (list of key-value pairs or a
                          string with such a list in JSON)
        """

        if type(query) is not list:
            try:
                self.query = json.loads(query)
            except ValueError:
                self.query = []
        else:
            self.query = query

        get_vars = {}
        for k, v in self.query:
            if v is not None:
                key = resource.prefix_selector(k)
                if key in get_vars:
                    value = get_vars[key]
                    if type(value) is list:
                        value.append(v)
                    else:
                        get_vars[key] = [value, v]
                else:
                    get_vars[key] = v

        self.resource = resource
        self.get_vars = get_vars

    # -------------------------------------------------------------------------
    def represent(self):
        """ Render the query representation for the given resource """

        default = ""

        get_vars = self.get_vars
        resource = self.resource
        if not get_vars:
            return default
        else:
            queries = S3URLQuery.parse(resource, get_vars)

        # Get alternative field labels
        labels = {}
        get_config = resource.get_config
        prefix = resource.prefix_selector
        for config in ("list_fields", "notify_fields"):
            fields = get_config(config, set())
            for f in fields:
                if type(f) is tuple:
                    labels[prefix(f[1])] = f[0]

        # Iterate over the sub-queries
        render = self._render
        substrings = []
        append = substrings.append
        for alias, subqueries in queries.items():

            for subquery in subqueries:
                s = render(resource, alias, subquery, labels=labels)
                if s:
                    append(s)

        if substrings:
            result = substrings[0]
            T = current.T
            for s in substrings[1:]:
                result = T("%s AND %s") % (result, s)
            return result
        else:
            return default

    # -------------------------------------------------------------------------
    @classmethod
    def _render(cls, resource, alias, query, invert=False, labels=None):
        """
            Recursively render a human-readable representation of a
            S3ResourceQuery.

            Args:
                resource: the CRUDResource
                query: the S3ResourceQuery
                invert: invert the query
        """

        T = current.T

        if not query:
            return None

        op = query.op

        l = query.left
        r = query.right
        render = lambda q, r=resource, a=alias, invert=False, labels=labels: \
                        cls._render(r, a, q, invert=invert, labels=labels)

        if op == query.AND:
            # Recurse AND
            l = render(l)
            r = render(r)
            if l is not None and r is not None:
                if invert:
                    result = T("NOT %s OR NOT %s") % (l, r)
                else:
                    result = T("%s AND %s") % (l, r)
            else:
                result = l if l is not None else r
        elif op == query.OR:
            # Recurse OR
            l = render(l)
            r = render(r)
            if l is not None and r is not None:
                if invert:
                    result = T("NOT %s AND NOT %s") % (l, r)
                else:
                    result = T("%s OR %s") % (l, r)
            else:
                result = l if l is not None else r
        elif op == query.NOT:
            # Recurse NOT
            result = render(l, invert=not invert)
        else:
            # Resolve the field selector against the resource
            try:
                rfield = l.resolve(resource)
            except (AttributeError, SyntaxError):
                return None

            # Convert the filter values into the field type
            try:
                values = cls._convert(rfield, r)
            except (TypeError, ValueError):
                values = r

            # Alias
            selector = l.name
            if labels and selector in labels:
                rfield.label = labels[selector]
            # @todo: for duplicate labels, show the table name
            #else:
                #tlabel = " ".join(s.capitalize() for s in rfield.tname.split("_")[1:])
                #rfield.label = "(%s) %s" % (tlabel, rfield.label)

            # Represent the values
            if values is None:
                values = T("None")
            else:
                list_type = rfield.ftype[:5] == "list:"
                renderer = rfield.represent
                if not callable(renderer):
                    renderer = s3_str
                if hasattr(renderer, "linkto"):
                    #linkto = renderer.linkto
                    renderer.linkto = None
                #else:
                #    #linkto = None

                is_list = type(values) is list

                try:
                    if is_list and hasattr(renderer, "bulk") and not list_type:
                        fvalues = renderer.bulk(values, list_type=False)
                        values = [fvalues[v] for v in values if v in fvalues]
                    elif list_type:
                        if is_list:
                            values = renderer(values)
                        else:
                            values = renderer([values])
                    else:
                        if is_list:
                            values = [renderer(v) for v in values]
                        else:
                            values = renderer(values)
                except:
                    values = s3_str(values)

            # Translate the query
            result = cls._translate_query(query, rfield, values, invert=invert)

        return result

    # -------------------------------------------------------------------------
    @classmethod
    def _convert(cls, rfield, value):
        """
            Convert a filter value according to the field type
            before representation

            Args:
                rfield: the S3ResourceField
                value: the value
        """

        if value is None:
            return value

        ftype = rfield.ftype
        if ftype[:5] == "list:":
            if ftype[5:8] in ("int", "ref"):
                ftype = int
            else:
                ftype = str
        elif ftype == "id" or ftype [:9] == "reference":
            ftype = int
        elif ftype == "integer":
            ftype = int
        elif ftype == "date":
            ftype = datetime.date
        elif ftype == "time":
            ftype = datetime.time
        elif ftype == "datetime":
            ftype = datetime.datetime
        elif ftype == "double":
            ftype = float
        elif ftype == "boolean":
            ftype = bool
        else:
            ftype = str

        convert = S3TypeConverter.convert
        if type(value) is list:
            output = []
            append = output.append
            for v in value:
                try:
                    append(convert(ftype, v))
                except (TypeError, ValueError):
                    continue
        else:
            try:
                output = convert(ftype, value)
            except (TypeError, ValueError):
                output = None
        return output

    # -------------------------------------------------------------------------
    @classmethod
    def _translate_query(cls, query, rfield, values, invert=False):
        """
            Translate the filter query into human-readable language

            Args:
                query: the S3ResourceQuery
                rfield: the S3ResourceField the query refers to
                values: the filter values
                invert: invert the operation
        """

        T = current.T

        # Value list templates
        vor = T("%s or %s")
        vand = T("%s and %s")

        # Operator templates
        otemplates = {
            query.LT: (query.GE, vand, "%(label)s < %(values)s"),
            query.LE: (query.GT, vand, "%(label)s <= %(values)s"),
            query.EQ: (query.NE, vor, T("%(label)s is %(values)s")),
            query.GE: (query.LT, vand, "%(label)s >= %(values)s"),
            query.GT: (query.LE, vand, "%(label)s > %(values)s"),
            query.NE: (query.EQ, vor, T("%(label)s != %(values)s")),
            query.LIKE: ("notlike", vor, T("%(label)s like %(values)s")),
            query.BELONGS: (query.NE, vor, T("%(label)s = %(values)s")),
            query.CONTAINS: ("notall", vand, T("%(label)s contains %(values)s")),
            query.ANYOF: ("notany", vor, T("%(label)s contains any of %(values)s")),
            "notall": (query.CONTAINS, vand, T("%(label)s does not contain %(values)s")),
            "notany": (query.ANYOF, vor, T("%(label)s does not contain %(values)s")),
            "notlike": (query.LIKE, vor, T("%(label)s not like %(values)s"))
        }

        # Quote values as necessary
        ftype = rfield.ftype
        if ftype in ("string", "text") or \
           ftype[:9] == "reference" or \
           ftype[:5] == "list:" and ftype[5:8] in ("str", "ref"):
            if type(values) is list:
                values = ['"%s"' % v for v in values]
            elif values is not None:
                values = '"%s"' % values
            else:
                values = current.messages["NONE"]

        # Render value list template
        def render_values(template=None, values=None):
            if not template or type(values) is not list:
                return str(values)
            elif not values:
                return "()"
            elif len(values) == 1:
                return values[0]
            else:
                return template % (", ".join(values[:-1]), values[-1])

        # Render the operator template
        op = query.op
        if op in otemplates:
            inversion, vtemplate, otemplate = otemplates[op]
            if invert:
                inversion, vtemplate, otemplate = otemplates[inversion]
            return otemplate % {"label": rfield.label,
                                "values":render_values(vtemplate, values),
                                }
        else:
            # Fallback to simple representation
            return query.represent(rfield.resource)

# =============================================================================
def s3_get_filter_opts(tablename,
                       fieldname = "name",
                       location_filter = False,
                       org_filter = False,
                       key = "id",
                       none = False,
                       orderby = None,
                       translate = False,
                       ):
    """
        Lazy options getter - this is useful when the expected number
        of options is significantly smaller than the number of records
        to iterate through

        NB unlike the built-in reverse lookup in S3OptionsFilter, this
           function does *not* check whether the options are actually
           in use - so it can be used to enforce filter options to be
           shown even if there are no records matching them.

        Args:
            tablename: the name of the lookup table
            fieldname: the name of the field to represent options with
            location_filter: whether to filter the values by location
            org_filter: whether to filter the values by root_org
            key: the option key field (if not "id", e.g. a super key)
            none: whether to include an option for None
            orderby: orderby-expression as alternative to alpha-sorting
                     of options in widget (=> set widget sort=False)
            translate: whether to translate the values
    """

    auth = current.auth
    table = current.s3db.table(tablename)

    if auth.s3_has_permission("read", table):
        query = auth.s3_accessible_query("read", table)
        if "deleted" in table.fields:
            query &= (table.deleted == False)
        if location_filter:
            location = current.session.s3.location_filter
            if location:
                query &= (table.location_id == location)
        if org_filter:
            root_org = auth.root_org()
            if root_org:
                query &= ((table.organisation_id == root_org) | \
                          (table.organisation_id == None))
            #else:
            #    query &= (table.organisation_id == None)
        if orderby is None:
            # Options are alpha-sorted later in widget
            odict = dict
        else:
            # Options-dict to retain order
            odict = OrderedDict
        rows = current.db(query).select(table[key],
                                        table[fieldname],
                                        orderby = orderby,
                                        )

        if translate:
            T = current.T
            opts = odict((row[key], T(row[fieldname])) for row in rows)
        else:
            opts = odict((row[key], row[fieldname]) for row in rows)
        if none:
            opts[None] = current.messages["NONE"]
    else:
        opts = {}
    return opts

# =============================================================================
def s3_set_default_filter(selector, value, tablename=None):
    """
        Set a default filter for selector.

        Args:
            selector: the field selector
            value: the value, can be a dict {operator: value},
                   a list of values, or a single value, or a
                   callable that returns any of these
            tablename: the tablename
    """

    s3 = current.response.s3

    filter_defaults = s3
    for level in ("filter_defaults", tablename):
        if level not in filter_defaults:
            filter_defaults[level] = {}
        filter_defaults = filter_defaults[level]
    filter_defaults[selector] = value

# END =========================================================================
