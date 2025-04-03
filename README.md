# MySQL_DDL_and_beyond
Simple application that simulates a storefront which needs to communicate with a MySQL database. The database has 2 tables: Order (parent) and Order details (child). In this particular program, DDL statements were written and executed to create a new schema. This is in addition to using CRUD operations in a transaction, with PreparedStatements.</br>
</br>
As an additional challenge, some of the input orders had their dates purposefully formatted incorrectly. The code needed extra design to parse and decide if dates were properly formatted. If they weren't, the data following it was ignored.</br>
</br>
Alongside an IDE, MySQL workbench was used to set up the initial table and verify operation results. Sample orders that were used for testing were stored and accessed in a .csv file.</br>
The .csv file used for unit testing can also be found in this repository.
