=============
addfield
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``addfield`` command adds a field with a constant value.


Syntax
============
addfield <field_name> <field_value>

* <field_name>: mandatory string. name of field to add
* <field_value>: mandatory string. value of new field

Example 1: Add a field named "foo" with the value "bar"
===========================================

The example show maximum 10 results from accounts index.

PPL query::

    os> source=accounts | fields firstname, age | addfield 'foo' 'bar';
    fetched rows / total rows = 4/4
    +-------------+-------+-------+
    | firstname   | age   | foo   |
    |-------------+-------+-------|
    | Amber       | 32    | bar   |
    | Hattie      | 36    | bar   |
    | Nanette     | 28    | bar   |
    | Dale        | 33    | bar   |
    +-------------+-------+-------+

Limitation
==========
The ``addfield`` command is not rewritten to OpenSearch DSL, it is only executed on the coordination node.
