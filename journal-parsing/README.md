# Purpose

Library to discover the journal for a schema and process the journal entries
Written to add ibmi support to debezium

# License

[Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0) for consistency with debezium

# Downstream projects

debezium-connector-ibmi


# No journal entries found check journalling is enabled and set to *BOTH

`dspfd FINACC`

```
    File is currently journaled . . . . . . . . :            Yes
    Current or last journal . . . . . . . . . . :            FIGJRN
      Library . . . . . . . . . . . . . . . . . :            F63QULDVES
    Journal images  . . . . . . . . . . . . . . : IMAGES     *BOTH

```

# Permissions
```
GRTOBJAUT OBJ(<JRNLIB>) OBJTYPE(*LIB) USER(<CDC_USER>) AUT(*EXECUTE)
GRTOBJAUT OBJ(<JRNLIB>/*ALL) OBJTYPE(*JRNRCV) USER(<CDC_USER>) AUT(*USE)
GRTOBJAUT OBJ(<JRNLIB>/<JRN>) OBJTYPE(*JRN) USER(<CDC_USER>) AUT(*USE *OBJEXIST)

GRTOBJAUT OBJ(<FIGLIB>) OBJTYPE(*LIB) USER(<CDC_USER>) AUT(*EXECUTE)
GRTOBJAUT OBJ(<FIGLIB>/*ALL) OBJTYPE(*FILE) USER(<CDC_USER>) AUT(*USE)
```
Where:

* `<JRNLIB>` is the library where the journal and receivers reside
* `<JRN>` is the journal name
* `<FIGLIB>` is the Figaro database library
* `<CDC_USER>` is the username of the CDC service account

## Reference:

https://www.ibm.com/docs/en/i/7.4?topic=commands-journal

https://www.ibm.com/docs/en/i/7.4?topic=information-layouts-variable-length-portion-journal-entries#rzakivarlength__TBLOBJLVL

Will need at least the authorities described for RTVJRNE.



| Command For object | Referenced object For library | Authority needed or directory |
| ------------------ | ----------------------------- | ----------------------------- |
| RTVJRNE | Journal | \*USE | \*EXECUTE |
| RTVJRNE | Journal if FILE(*ALLFILE) is specified, no object selection is specified, the specified object has been deleted from the system, the specified object has never been journaled, \*IGNFILSLT or \*IGNOBJSLT is specified for any selected journal codes, or when OBJJID is specified, or the journal is a remote journal. | \*OBJEXIST, \*USE	\*EXECUTE |
| RTVJRNE | Journal receiver | \*USE \*EXECUTE |
| RTVJRNE | Nonintegrated file system object if specified | \*USE	\*EXECUTE |
| RTVJRNE | Integrated file system object if specified | \*R (It can be \*X as well if object is a directory and SUBTREE (\*ALL) is specified) | \*X |


# Debugging

use table query to investigate journal entries

see: https://www.ibm.com/docs/en/i/7.4?topic=services-display-journal-table-function and https://dawnmayi.com/2010/11/23/using-sql-to-interrogate-journal-objects/

```
select * from table (Display_Journal(
  'F63QULDVES',     'FIGJRN',  -- Journal library and name
  ' ','*CURCHAIN',        -- Receiver library and name
  CAST('2023-04-06-00.00.00.000000' as TIMESTAMP), -- Starting timestamp
  CAST(null as DECIMAL(21,0)), -- Starting sequence number
  '',              -- Journal codes
  '',              -- Journal entries
  '',  '',         -- Object library, Object name - library alone OK, name also needs library
  '*FILE', '*ALL', -- Object type, Object member
  '',              -- User
  '',              -- Job
  '',              -- Program
  ''       			-- EOF delay
) ) as x;
```

or simply:

```
SELECT entry_timestamp, receiver_name, sequence_number, journal_code, journal_entry_type, object
  FROM TABLE (QSYS2.DISPLAY_JOURNAL( 'F63QULDVES', 'FIGJRN')) AS JT
  ORDER BY entry_timestamp desc;
```

## list of journals

```
SELECT
	journal_receiver_name,
	first_sequence_number,
	LAST_sequence_number,
	attach_timestamp
FROM
	QSYS2.JOURNAL_RECEIVER_INFO
WHERE
	journal_name = 'FIGJRN'
	AND JOURNAL_RECEIVER_LIBRARY = 'F63QUALDB4'
	AND status = 'ONLINE'
ORDER BY
	attach_timestamp ASC ;
```

# TODO

Improved way of fetching the journal information

https://www.ibm.com/docs/en/i/7.4?topic=ssw_ibm_i_74/apis/qlirlibd.htm

That API is actually a program. You can still call it, but the mechanism is slightly different in JTOpen
The program name is QLIRLIBD
The library is likely QSYS, but *LIBL should work


# Limitations

Can only decode the journal data if the table structure is currently the same

[Unable to decode table structure changes](https://ibm-power-systems.ideas.ibm.com/ideas/IBMI-I-3211) as they are not documented D.CG or table creation D.CT
however we can detect what table changed

Currently unable to capture clob/blob/xml the memory pointer supplied is not accessible with the current mechanism to fetch the journal data


When the library is pointing to the wrong journal, it can be fixed with:

```
STRJRNLIB LIB(<library>) JRN(<journal)
```

## Filtering on tables does not work properly across delete and create

Testing suggests that the filtering only applies to the table that exists during the call to fetch entries

# gotyas

From the documentation: https://www.ibm.com/docs/en/i/7.5?topic=ssw_ibm_i_75/apis/QJORJRNE.html

## Restrictions - at top of page
If the sequence number is reset in the range of the receivers specified, the first occurrence of starting sequence number or ending sequence number is used if these key fields are specified.

## Starting journal receiver name. - section "Receiver Range Format"
Note: For journal receivers with reset sequence numbers in the chain, the QjoRetrieveJournalEntries API may return the same journal entries for repeated API calls. To avoid receiving the same journal entries, change the starting journal receiver name field to indicate the next receiver in the chain after the initial call to the API.

So unless the start receiver is set we can end up in a loop, we can't simply use the `*CURCHAIN` to step over the journal entries

# seechange notes
a CR in development e.g. `CR 076737 / 37` has a schema `O#07673737` the shema isn't journaled so this won't work for CRs that haven't been promoted to holding or quality
A CR in holding/quality will use the normal schema for that environment
