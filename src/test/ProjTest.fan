//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack
using concurrent
using sql

**
** ProjTest
**
class ProjTest : Test
{
  Void testMultipleProjects()
  {
    echo("===============================================================")
    echo

    pool := HavenPool
    {
      it.uri      = "jdbc:postgresql://localhost/postgres"
      it.username = "xbd"
      it.password = "s3crkEt"
      it.maxConns = 10
    }

    // set up multiple haven projects
    Haven[] havens := Str["proj1", "proj2"].map |Str proj->Haven|
    {
      //echo("Creating ${proj}")
      //pool.execute(|SqlConn conn| { TestDataLoader.nuke(conn, proj) })

      haven := Haven(proj, pool)
      haven.init
      return haven
    }

    // load the data
    //td := TestData()
    //havens.each |haven|
    //{
    //  TestDataLoader.loadSpecs(haven, td)
    //  TestDataLoader.loadRecs(haven, td)
    //}

    runQueries(pool, havens)

    // done
    pool.close

    echo
    echo("===============================================================")
  }

  private Void runQueries(SqlConnPool pool, Haven[] havens)
  {
    actorPool := ActorPool {it.name="ProjTest"}
    actor := Actor(actorPool) |ReadAll r->Int|
    {
      start := DateTime.now
      Dict[] recs := r.haven.readAll(r.filter)
      time := (DateTime.now - start).ticks
      //echo("${r.haven.projName} ${recs.size}: ${r.filter}")
      return time
    }

    filters := makeFilters(pool)

    elapsed := Future[,]
    //count := 10_000
    count := 1000
    for (i := 0; i < count; i++)
    {
      r := ReadAll {
        it.haven  = havens [Int.random(0..<havens.size)]
        it.filter = filters[Int.random(0..<filters.size)] }
      elapsed.add(actor.send(r))
    }

    actorPool.stop
    actorPool.join

    Int total := elapsed.reduce(0) |Obj r, Future v->Obj| {
      return (Int)r + (Int)v.get
    }
    avg := Duration(total/count).ticks
    Float avgMs := avg.toFloat/1000000.0f

    echo
    echo("runQueries: avg ${avgMs}ms")
    echo
  }

  private Filter[] makeFilters(SqlConnPool pool)
  {
    Filter[] filters := [,]

    pool.execute(|SqlConn conn|
    {
      conn.sql("set search_path to proj1").execute
      stmt := conn.sql("select distinct path_, target from path_ref where path_ not in ('id','spec')")
      stmt.query.each |r|
      {
        Str path := r->path_
        path = path.replace(".", "->")
        Str target := r->target
        filters.add(Filter("$path == @$target"))
        //echo(filters[-1])
      }
      stmt.close
    })

    return filters
  }
}

**************************************************************************
** ReadAll
**************************************************************************

const class ReadAll
{
  new make(|This|? f) { if (f != null) f(this) }

  const Haven haven
  const Filter filter
}
