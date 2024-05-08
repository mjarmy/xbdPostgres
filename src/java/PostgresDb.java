//
// Copyright (c) 2023, SkyFoundry LLC
// All Rights Reserved
//
// History:
//   20 Mar 2024  Mike Jarmy  Creation
//

package fan.xbdPostgres;

import fan.sys.*;

import java.sql.*;
import java.util.*;

public class PostgresDb extends FanObj
{

//////////////////////////////////////////////////////////////////////////
// Lifecycle
//////////////////////////////////////////////////////////////////////////

  public static PostgresDb make()
  {
    PostgresDb self = new PostgresDb();
    //make$(self, arg);
    return self;
  }

  public final Type typeof() { return typeof; }
  private static final Type typeof = Type.find("xbdPostgres::PostgresDb");

//////////////////////////////////////////////////////////////////////////
// Native
//////////////////////////////////////////////////////////////////////////

  public void open(String uri, String username, String password)
      throws Exception
  {
    Properties props = new Properties();
    props.setProperty("user", username);
    props.setProperty("password", password);
    this.conn = DriverManager.getConnection(uri, props);
    conn.setAutoCommit(false);

    this.insertRec = conn.prepareStatement(
      "insert into rec (id, hayson) values (?, ?::jsonb)");

    this.insertArrow = conn.prepareStatement(
      "insert into arrow (from_id, to_path, to_id) values (?, ?, ?)");

    this.insertSpec = conn.prepareStatement(
      "insert into spec (qname, inherits_from) values (?, ?)");

    this.insertFoo = conn.prepareStatement(
      "insert into foo (id, paths, obj) values (?, ?, ?::jsonb)");
  }

  public void close() throws Exception
  {
    insertRec.close();
    insertArrow.close();
    insertSpec.close();
    insertFoo.close();
    conn.close();
  }

  public void writeSpec(String name, fan.sys.List inherits)
      throws Exception
  {
    insertSpec.setString(1, name);
    insertSpec.setObject(2, toStringArray(inherits));
    insertSpec.executeUpdate();
    conn.commit();
  }

  public void writeRec(String id, String hayson, fan.sys.List arrows)
      throws Exception
  {
    insertRec.setString(1, id);
    insertRec.setString(2, hayson);
    insertRec.executeUpdate();

    for (int i = 0; i < arrows.size(); i++)
    {
      Arrow a = (Arrow) arrows.get(i);
      insertArrow.setString(1, id);
      insertArrow.setString(2, a.toPath);
      insertArrow.setString(3, a.toId);
      insertArrow.addBatch();
    }
    insertArrow.executeBatch();

    conn.commit();
  }

  public void writeFoo(String id, fan.sys.List paths, String hayson)
      throws Exception
  {
    insertFoo.setString(1, id);
    insertFoo.setObject(2, toStringArray(paths));
    insertFoo.setString(3, hayson);
    insertFoo.executeUpdate();

    conn.commit();
  }

  private static String[] toStringArray(fan.sys.List list)
  {
    String[] arr = new String[(int)list.size()];
    for (int i = 0; i < list.size(); i++)
      arr[i] = (String) list.get(i);
    return arr;
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private Connection conn;
  private PreparedStatement insertRec;
  private PreparedStatement insertArrow;
  private PreparedStatement insertSpec;
  private PreparedStatement insertFoo;
}

