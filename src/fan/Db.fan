//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using sql

****************************************************************
** Db
****************************************************************

class Db
{
  Void open(Str uri, Str? username, Str? password)
  {
    conn = SqlConn.open(uri, username, password)

    insertSpecStmt = conn.sql(
      "insert into spec (qname, inherits_from)
       values (@qname, @inheritsFrom)").prepare
  }

  Void close()
  {
    insertSpecStmt.close
    insertSpecStmt = null

    conn.close
    conn = null
  }

  Void insertSpec(Str qname, Str[] inheritsFrom)
  {
    insertSpecStmt.execute([
      "qname":qname,
      "inheritsFrom": inheritsFrom
    ])
  }

  //-----------------------------------------------
  // Fields
  //-----------------------------------------------

  private SqlConn? conn
  private Statement? insertSpecStmt
}
