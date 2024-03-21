//
// Copyright (c) 2023, SkyFoundry LLC
// All Rights Reserved
//
// History:
//   20 Mar 2024  Mike Jarmy  Creation
//

package fan.xbdPostgres;

import fan.sys.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;

public class DbXetoSpec
{
//////////////////////////////////////////////////////////////////////////
// Lifecycle
//////////////////////////////////////////////////////////////////////////

  public final Type typeof() { return typeof; }
  private static final Type typeof = Type.find("xbdPostgres::DbXetoSpec");

//////////////////////////////////////////////////////////////////////////
// Native
//////////////////////////////////////////////////////////////////////////

  public void openConn() throws Exception
  {
    String url = "jdbc:postgresql://localhost/postgres";
    String user = "xbd";
    String password = "s3crkEt";

    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    this.conn = DriverManager.getConnection(url, props);
    conn.setAutoCommit(false);

    //conn.sql("insert into spec (qname) values (@qname)")
    //  .execute(["qname":spec.qname])
  }

  public void closeConn() throws Exception
  {
    //insertSpec.close();
    conn.close();
  }

  public void writeSpec(String spec)
  {
    System.out.println(spec);
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private Connection conn;
  //private PreparedStatement insertSpec;
}

