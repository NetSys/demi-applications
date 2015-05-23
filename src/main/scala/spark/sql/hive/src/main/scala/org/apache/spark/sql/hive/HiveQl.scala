/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan.PlanUtils

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types._

/* Implicit conversions */
import scala.collection.JavaConversions._

/**
 * Used when we need to start parsing the AST before deciding that we are going to pass the command
 * back for Hive to execute natively.  Will be replaced with a native command that contains the
 * cmd string.
 */
private[hive] case object NativePlaceholder extends Command

private[hive] case class ShellCommand(cmd: String) extends Command

private[hive] case class SourceCommand(filePath: String) extends Command

private[hive] case class AddJar(jarPath: String) extends Command

private[hive] case class AddFile(filePath: String) extends Command

/** Provides a mapping from HiveQL statements to catalyst logical plans and expression trees. */
private[hive] object HiveQl {
  protected val nativeCommands = Seq(
    "TOK_DESCFUNCTION",
    "TOK_DESCDATABASE",
    "TOK_SHOW_TABLESTATUS",
    "TOK_SHOWDATABASES",
    "TOK_SHOWFUNCTIONS",
    "TOK_SHOWINDEXES",
    "TOK_SHOWINDEXES",
    "TOK_SHOWPARTITIONS",
    "TOK_SHOWTABLES",

    "TOK_LOCKTABLE",
    "TOK_SHOWLOCKS",
    "TOK_UNLOCKTABLE",

    "TOK_CREATEROLE",
    "TOK_DROPROLE",
    "TOK_GRANT",
    "TOK_GRANT_ROLE",
    "TOK_REVOKE",
    "TOK_SHOW_GRANT",
    "TOK_SHOW_ROLE_GRANT",

    "TOK_CREATEFUNCTION",
    "TOK_DROPFUNCTION",

    "TOK_ANALYZE",
    "TOK_ALTERDATABASE_PROPERTIES",
    "TOK_ALTERINDEX_PROPERTIES",
    "TOK_ALTERINDEX_REBUILD",
    "TOK_ALTERTABLE_ADDCOLS",
    "TOK_ALTERTABLE_ADDPARTS",
    "TOK_ALTERTABLE_ALTERPARTS",
    "TOK_ALTERTABLE_ARCHIVE",
    "TOK_ALTERTABLE_CLUSTER_SORT",
    "TOK_ALTERTABLE_DROPPARTS",
    "TOK_ALTERTABLE_PARTITION",
    "TOK_ALTERTABLE_PROPERTIES",
    "TOK_ALTERTABLE_RENAME",
    "TOK_ALTERTABLE_RENAMECOL",
    "TOK_ALTERTABLE_REPLACECOLS",
    "TOK_ALTERTABLE_SKEWED",
    "TOK_ALTERTABLE_TOUCH",
    "TOK_ALTERTABLE_UNARCHIVE",
    "TOK_ANALYZE",
    "TOK_CREATEDATABASE",
    "TOK_CREATEFUNCTION",
    "TOK_CREATEINDEX",
    "TOK_DROPDATABASE",
    "TOK_DROPINDEX",
    "TOK_DROPTABLE",
    "TOK_MSCK",

    // TODO(marmbrus): Figure out how view are expanded by hive, as we might need to handle this.
    "TOK_ALTERVIEW_ADDPARTS",
    "TOK_ALTERVIEW_AS",
    "TOK_ALTERVIEW_DROPPARTS",
    "TOK_ALTERVIEW_PROPERTIES",
    "TOK_ALTERVIEW_RENAME",
    "TOK_CREATEVIEW",
    "TOK_DROPVIEW",

    "TOK_EXPORT",
    "TOK_IMPORT",
    "TOK_LOAD",

    "TOK_SWITCHDATABASE"
  )

  // Commands that we do not need to explain.
  protected val noExplainCommands = Seq(
    "TOK_CREATETABLE",
    "TOK_DESCTABLE"
  ) ++ nativeCommands

  /**
   * A set of implicit transformations that allow Hive ASTNodes to be rewritten by transformations
   * similar to [[catalyst.trees.TreeNode]].
   *
   * Note that this should be considered very experimental and is not indented as a replacement
   * for TreeNode.  Primarily it should be noted ASTNodes are not immutable and do not appear to
   * have clean copy semantics.  Therefore, users of this class should take care when
   * copying/modifying trees that might be used elsewhere.
   */
  implicit class TransformableNode(n: ASTNode) {
    /**
     * Returns a copy of this node where `rule` has been recursively applied to it and all of its
     * children.  When `rule` does not apply to a given node it is left unchanged.
     * @param rule the function use to transform this nodes children
     */
    def transform(rule: PartialFunction[ASTNode, ASTNode]): ASTNode = {
      try {
        val afterRule = rule.applyOrElse(n, identity[ASTNode])
        afterRule.withChildren(
          nilIfEmpty(afterRule.getChildren)
            .asInstanceOf[Seq[ASTNode]]
            .map(ast => Option(ast).map(_.transform(rule)).orNull))
      } catch {
        case e: Exception =>
          println(dumpTree(n))
          throw e
      }
    }

    /**
     * Returns a scala.Seq equivalent to [s] or Nil if [s] is null.
     */
    private def nilIfEmpty[A](s: java.util.List[A]): Seq[A] =
      Option(s).map(_.toSeq).getOrElse(Nil)

    /**
     * Returns this ASTNode with the text changed to `newText`.
     */
    def withText(newText: String): ASTNode = {
      n.token.asInstanceOf[org.antlr.runtime.CommonToken].setText(newText)
      n
    }

    /**
     * Returns this ASTNode with the children changed to `newChildren`.
     */
    def withChildren(newChildren: Seq[ASTNode]): ASTNode = {
      (1 to n.getChildCount).foreach(_ => n.deleteChild(0))
      n.addChildren(newChildren)
      n
    }

    /**
     * Throws an error if this is not equal to other.
     *
     * Right now this function only checks the name, type, text and children of the node
     * for equality.
     */
    def checkEquals(other: ASTNode) {
      def check(field: String, f: ASTNode => Any) = if (f(n) != f(other)) {
        sys.error(s"$field does not match for trees. " +
          s"'${f(n)}' != '${f(other)}' left: ${dumpTree(n)}, right: ${dumpTree(other)}")
      }
      check("name", _.getName)
      check("type", _.getType)
      check("text", _.getText)
      check("numChildren", n => nilIfEmpty(n.getChildren).size)

      val leftChildren = nilIfEmpty(n.getChildren).asInstanceOf[Seq[ASTNode]]
      val rightChildren = nilIfEmpty(other.getChildren).asInstanceOf[Seq[ASTNode]]
      leftChildren zip rightChildren foreach {
        case (l,r) => l checkEquals r
      }
    }
  }

  class ParseException(sql: String, cause: Throwable)
    extends Exception(s"Failed to parse: $sql", cause)

  class SemanticException(msg: String)
    extends Exception(s"Error in semantic analysis: $msg")

  /**
   * Returns the AST for the given SQL string.
   */
  def getAst(sql: String): ASTNode = ParseUtils.findRootNonNullToken((new ParseDriver).parse(sql))

  /** Returns a LogicalPlan for a given HiveQL string. */
  def parseSql(sql: String): LogicalPlan = {
    try {
      if (sql.trim.toLowerCase.startsWith("set")) {
        // Split in two parts since we treat the part before the first "="
        // as key, and the part after as value, which may contain other "=" signs.
        sql.trim.drop(3).split("=", 2).map(_.trim) match {
          case Array("") => // "set"
            SetCommand(None, None)
          case Array(key) => // "set key"
            SetCommand(Some(key), None)
          case Array(key, value) => // "set key=value"
            SetCommand(Some(key), Some(value))
        }
      } else if (sql.trim.toLowerCase.startsWith("cache table")) {
        CacheCommand(sql.trim.drop(12).trim, true)
      } else if (sql.trim.toLowerCase.startsWith("uncache table")) {
        CacheCommand(sql.trim.drop(14).trim, false)
      } else if (sql.trim.toLowerCase.startsWith("add jar")) {
        AddJar(sql.trim.drop(8))
      } else if (sql.trim.toLowerCase.startsWith("add file")) {
        AddFile(sql.trim.drop(9))
      } else if (sql.trim.toLowerCase.startsWith("dfs")) {
        NativeCommand(sql)
      } else if (sql.trim.startsWith("source")) {
        SourceCommand(sql.split(" ").toSeq match { case Seq("source", filePath) => filePath })
      } else if (sql.trim.startsWith("!")) {
        ShellCommand(sql.drop(1))
      } else {
        val tree = getAst(sql)

        if (nativeCommands contains tree.getText) {
          NativeCommand(sql)
        } else {
          nodeToPlan(tree) match {
            case NativePlaceholder => NativeCommand(sql)
            case other => other
          }
        }
      }
    } catch {
      case e: Exception => throw new ParseException(sql, e)
      case e: NotImplementedError => sys.error(
        s"""
          |Unsupported language features in query: $sql
          |${dumpTree(getAst(sql))}
        """.stripMargin)
    }
  }

  def parseDdl(ddl: String): Seq[Attribute] = {
    val tree =
      try {
        ParseUtils.findRootNonNullToken(
          (new ParseDriver).parse(ddl, null /* no context required for parsing alone */))
      } catch {
        case pe: org.apache.hadoop.hive.ql.parse.ParseException =>
          throw new RuntimeException(s"Failed to parse ddl: '$ddl'", pe)
      }
    assert(tree.asInstanceOf[ASTNode].getText == "TOK_CREATETABLE", "Only CREATE TABLE supported.")
    val tableOps = tree.getChildren
    val colList =
      tableOps
        .find(_.asInstanceOf[ASTNode].getText == "TOK_TABCOLLIST")
        .getOrElse(sys.error("No columnList!")).getChildren

    colList.map(nodeToAttribute)
  }

  /** Extractor for matching Hive's AST Tokens. */
  object Token {
    /** @return matches of the form (tokenName, children). */
    def unapply(t: Any): Option[(String, Seq[ASTNode])] = t match {
      case t: ASTNode =>
        Some((t.getText,
          Option(t.getChildren).map(_.toList).getOrElse(Nil).asInstanceOf[Seq[ASTNode]]))
      case _ => None
    }
  }

  protected def getClauses(clauseNames: Seq[String], nodeList: Seq[ASTNode]): Seq[Option[Node]] = {
    var remainingNodes = nodeList
    val clauses = clauseNames.map { clauseName =>
      val (matches, nonMatches) = remainingNodes.partition(_.getText.toUpperCase == clauseName)
      remainingNodes = nonMatches ++ (if (matches.nonEmpty) matches.tail else Nil)
      matches.headOption
    }

    assert(remainingNodes.isEmpty,
      s"Unhandled clauses: ${remainingNodes.map(dumpTree(_)).mkString("\n")}")
    clauses
  }

  def getClause(clauseName: String, nodeList: Seq[Node]) =
    getClauseOption(clauseName, nodeList).getOrElse(sys.error(
      s"Expected clause $clauseName missing from ${nodeList.map(dumpTree(_)).mkString("\n")}"))

  def getClauseOption(clauseName: String, nodeList: Seq[Node]): Option[Node] = {
    nodeList.filter { case ast: ASTNode => ast.getText == clauseName } match {
      case Seq(oneMatch) => Some(oneMatch)
      case Seq() => None
      case _ => sys.error(s"Found multiple instances of clause $clauseName")
    }
  }

  protected def nodeToAttribute(node: Node): Attribute = node match {
    case Token("TOK_TABCOL", Token(colName, Nil) :: dataType :: Nil) =>
      AttributeReference(colName, nodeToDataType(dataType), true)()

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  protected def nodeToDataType(node: Node): DataType = node match {
    case Token("TOK_DECIMAL", Nil) => DecimalType
    case Token("TOK_BIGINT", Nil) => LongType
    case Token("TOK_INT", Nil) => IntegerType
    case Token("TOK_TINYINT", Nil) => ByteType
    case Token("TOK_SMALLINT", Nil) => ShortType
    case Token("TOK_BOOLEAN", Nil) => BooleanType
    case Token("TOK_STRING", Nil) => StringType
    case Token("TOK_FLOAT", Nil) => FloatType
    case Token("TOK_DOUBLE", Nil) => DoubleType
    case Token("TOK_TIMESTAMP", Nil) => TimestampType
    case Token("TOK_BINARY", Nil) => BinaryType
    case Token("TOK_LIST", elementType :: Nil) => ArrayType(nodeToDataType(elementType))
    case Token("TOK_STRUCT",
           Token("TOK_TABCOLLIST", fields) :: Nil) =>
      StructType(fields.map(nodeToStructField))
    case Token("TOK_MAP",
           keyType ::
           valueType :: Nil) =>
      MapType(nodeToDataType(keyType), nodeToDataType(valueType))
    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for DataType:\n ${dumpTree(a).toString} ")
  }

  protected def nodeToStructField(node: Node): StructField = node match {
    case Token("TOK_TABCOL",
           Token(fieldName, Nil) ::
           dataType :: Nil) =>
      StructField(fieldName, nodeToDataType(dataType), nullable = true)
    case Token("TOK_TABCOL",
           Token(fieldName, Nil) ::
             dataType ::
             _ /* comment */:: Nil) =>
      StructField(fieldName, nodeToDataType(dataType), nullable = true)
    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for StructField:\n ${dumpTree(a).toString} ")
  }

  protected def nameExpressions(exprs: Seq[Expression]): Seq[NamedExpression] = {
    exprs.zipWithIndex.map {
      case (ne: NamedExpression, _) => ne
      case (e, i) => Alias(e, s"c_$i")()
    }
  }

  protected def extractDbNameTableName(tableNameParts: Node): (Option[String], String) = {
    val (db, tableName) =
      tableNameParts.getChildren.map { case Token(part, Nil) => cleanIdentifier(part) } match {
        case Seq(tableOnly) => (None, tableOnly)
        case Seq(databaseName, table) => (Some(databaseName), table)
      }

    (db, tableName)
  }

  protected def nodeToPlan(node: Node): LogicalPlan = node match {
    // Just fake explain for any of the native commands.
    case Token("TOK_EXPLAIN", explainArgs)
      if noExplainCommands.contains(explainArgs.head.getText) =>
      ExplainCommand(NoRelation)
    case Token("TOK_EXPLAIN", explainArgs) =>
      // Ignore FORMATTED if present.
      val Some(query) :: _ :: _ :: Nil =
        getClauses(Seq("TOK_QUERY", "FORMATTED", "EXTENDED"), explainArgs)
      // TODO: support EXTENDED?
      ExplainCommand(nodeToPlan(query))

    case Token("TOK_DESCTABLE", describeArgs) =>
      // Reference: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
      val Some(tableType) :: formatted :: extended :: pretty :: Nil =
        getClauses(Seq("TOK_TABTYPE", "FORMATTED", "EXTENDED", "PRETTY"), describeArgs)
      if (formatted.isDefined || pretty.isDefined) {
        // FORMATTED and PRETTY are not supported and this statement will be treated as
        // a Hive native command.
        NativePlaceholder
      } else {
        tableType match {
          case Token("TOK_TABTYPE", nameParts) if nameParts.size == 1 => {
            nameParts.head match {
              case Token(".", dbName :: tableName :: Nil) =>
                // It is describing a table with the format like "describe db.table".
                // TODO: Actually, a user may mean tableName.columnName. Need to resolve this issue.
                val (db, tableName) = extractDbNameTableName(nameParts.head)
                DescribeCommand(
                  UnresolvedRelation(db, tableName, None), extended.isDefined)
              case Token(".", dbName :: tableName :: colName :: Nil) =>
                // It is describing a column with the format like "describe db.table column".
                NativePlaceholder
              case tableName =>
                // It is describing a table with the format like "describe table".
                DescribeCommand(
                  UnresolvedRelation(None, tableName.getText, None),
                  extended.isDefined)
            }
          }
          // All other cases.
          case _ => NativePlaceholder
        }
      }

    case Token("TOK_CREATETABLE", children)
        if children.collect { case t@Token("TOK_QUERY", _) => t }.nonEmpty =>
      // TODO: Parse other clauses.
      // Reference: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
      val (
          Some(tableNameParts) ::
          _ /* likeTable */ ::
          Some(query) +:
          notImplemented) =
        getClauses(
          Seq(
            "TOK_TABNAME",
            "TOK_LIKETABLE",
            "TOK_QUERY",
            "TOK_IFNOTEXISTS",
            "TOK_TABLECOMMENT",
            "TOK_TABCOLLIST",
            "TOK_TABLEPARTCOLS", // Partitioned by
            "TOK_TABLEBUCKETS", // Clustered by
            "TOK_TABLESKEWED", // Skewed by
            "TOK_TABLEROWFORMAT",
            "TOK_TABLESERIALIZER",
            "TOK_FILEFORMAT_GENERIC", // For file formats not natively supported by Hive.
            "TOK_TBLSEQUENCEFILE", // Stored as SequenceFile
            "TOK_TBLTEXTFILE", // Stored as TextFile
            "TOK_TBLRCFILE", // Stored as RCFile
            "TOK_TBLORCFILE", // Stored as ORC File
            "TOK_TABLEFILEFORMAT", // User-provided InputFormat and OutputFormat
            "TOK_STORAGEHANDLER", // Storage handler
            "TOK_TABLELOCATION",
            "TOK_TABLEPROPERTIES"),
          children)
      if (notImplemented.exists(token => !token.isEmpty)) {
        throw new NotImplementedError(
          s"Unhandled clauses: ${notImplemented.flatten.map(dumpTree(_)).mkString("\n")}")
      }

      val (db, tableName) = extractDbNameTableName(tableNameParts)

      InsertIntoCreatedTable(db, tableName, nodeToPlan(query))

    // If its not a "CREATE TABLE AS" like above then just pass it back to hive as a native command.
    case Token("TOK_CREATETABLE", _) => NativePlaceholder

    case Token("TOK_QUERY",
           Token("TOK_FROM", fromClause :: Nil) ::
           insertClauses) =>

      // Return one query for each insert clause.
      val queries = insertClauses.map { case Token("TOK_INSERT", singleInsert) =>
        val (
            intoClause ::
            destClause ::
            selectClause ::
            selectDistinctClause ::
            whereClause ::
            groupByClause ::
            orderByClause ::
            havingClause ::
            sortByClause ::
            clusterByClause ::
            distributeByClause ::
            limitClause ::
            lateralViewClause :: Nil) = {
          getClauses(
            Seq(
              "TOK_INSERT_INTO",
              "TOK_DESTINATION",
              "TOK_SELECT",
              "TOK_SELECTDI",
              "TOK_WHERE",
              "TOK_GROUPBY",
              "TOK_ORDERBY",
              "TOK_HAVING",
              "TOK_SORTBY",
              "TOK_CLUSTERBY",
              "TOK_DISTRIBUTEBY",
              "TOK_LIMIT",
              "TOK_LATERAL_VIEW"),
            singleInsert)
        }

        val relations = nodeToRelation(fromClause)
        val withWhere = whereClause.map { whereNode =>
          val Seq(whereExpr) = whereNode.getChildren.toSeq
          Filter(nodeToExpr(whereExpr), relations)
        }.getOrElse(relations)

        val select =
          (selectClause orElse selectDistinctClause).getOrElse(sys.error("No select clause."))

        // Script transformations are expressed as a select clause with a single expression of type
        // TOK_TRANSFORM
        val transformation = select.getChildren.head match {
          case Token("TOK_SELEXPR",
                 Token("TOK_TRANSFORM",
                   Token("TOK_EXPLIST", inputExprs) ::
                   Token("TOK_SERDE", Nil) ::
                   Token("TOK_RECORDWRITER", writerClause) ::
                   // TODO: Need to support other types of (in/out)put
                   Token(script, Nil) ::
                   Token("TOK_SERDE", serdeClause) ::
                   Token("TOK_RECORDREADER", readerClause) ::
                   outputClause :: Nil) :: Nil) =>

            val output = outputClause match {
              case Token("TOK_ALIASLIST", aliases) =>
                aliases.map { case Token(name, Nil) => AttributeReference(name, StringType)() }
              case Token("TOK_TABCOLLIST", attributes) =>
                attributes.map { case Token("TOK_TABCOL", Token(name, Nil) :: dataType :: Nil) =>
                  AttributeReference(name, nodeToDataType(dataType))() }
            }
            val unescapedScript = BaseSemanticAnalyzer.unescapeSQLString(script)

            Some(
              logical.ScriptTransformation(
                inputExprs.map(nodeToExpr),
                unescapedScript,
                output,
                withWhere))
          case _ => None
        }

        val withLateralView = lateralViewClause.map { lv =>
          val Token("TOK_SELECT",
          Token("TOK_SELEXPR", clauses) :: Nil) = lv.getChildren.head

          val alias =
            getClause("TOK_TABALIAS", clauses).getChildren.head.asInstanceOf[ASTNode].getText

          Generate(
            nodesToGenerator(clauses),
            join = true,
            outer = false,
            Some(alias.toLowerCase),
            withWhere)
        }.getOrElse(withWhere)

        // The projection of the query can either be a normal projection, an aggregation
        // (if there is a group by) or a script transformation.
        val withProject = transformation.getOrElse {
          // Not a transformation so must be either project or aggregation.
          val selectExpressions = nameExpressions(select.getChildren.flatMap(selExprNodeToExpr))

          groupByClause match {
            case Some(groupBy) =>
              Aggregate(groupBy.getChildren.map(nodeToExpr), selectExpressions, withLateralView)
            case None =>
              Project(selectExpressions, withLateralView)
          }
        }

        val withDistinct =
          if (selectDistinctClause.isDefined) Distinct(withProject) else withProject

        val withHaving = havingClause.map { h =>
          val havingExpr = h.getChildren.toSeq match { case Seq(hexpr) => nodeToExpr(hexpr) }
          // Note that we added a cast to boolean. If the expression itself is already boolean,
          // the optimizer will get rid of the unnecessary cast.
          Filter(Cast(havingExpr, BooleanType), withDistinct)
        }.getOrElse(withDistinct)

        val withSort =
          (orderByClause, sortByClause, distributeByClause, clusterByClause) match {
            case (Some(totalOrdering), None, None, None) =>
              Sort(totalOrdering.getChildren.map(nodeToSortOrder), withHaving)
            case (None, Some(perPartitionOrdering), None, None) =>
              SortPartitions(perPartitionOrdering.getChildren.map(nodeToSortOrder), withHaving)
            case (None, None, Some(partitionExprs), None) =>
              Repartition(partitionExprs.getChildren.map(nodeToExpr), withHaving)
            case (None, Some(perPartitionOrdering), Some(partitionExprs), None) =>
              SortPartitions(perPartitionOrdering.getChildren.map(nodeToSortOrder),
                Repartition(partitionExprs.getChildren.map(nodeToExpr), withHaving))
            case (None, None, None, Some(clusterExprs)) =>
              SortPartitions(clusterExprs.getChildren.map(nodeToExpr).map(SortOrder(_, Ascending)),
                Repartition(clusterExprs.getChildren.map(nodeToExpr), withHaving))
            case (None, None, None, None) => withHaving
            case _ => sys.error("Unsupported set of ordering / distribution clauses.")
          }

        val withLimit =
          limitClause.map(l => nodeToExpr(l.getChildren.head))
            .map(Limit(_, withSort))
            .getOrElse(withSort)

        // TOK_INSERT_INTO means to add files to the table.
        // TOK_DESTINATION means to overwrite the table.
        val resultDestination =
          (intoClause orElse destClause).getOrElse(sys.error("No destination found."))
        val overwrite = if (intoClause.isEmpty) true else false
        nodeToDest(
          resultDestination,
          withLimit,
          overwrite)
      }

      // If there are multiple INSERTS just UNION them together into on query.
      queries.reduceLeft(Union)

    case Token("TOK_UNION", left :: right :: Nil) => Union(nodeToPlan(left), nodeToPlan(right))

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  val allJoinTokens = "(TOK_.*JOIN)".r
  val laterViewToken = "TOK_LATERAL_VIEW(.*)".r
  def nodeToRelation(node: Node): LogicalPlan = node match {
    case Token("TOK_SUBQUERY",
           query :: Token(alias, Nil) :: Nil) =>
      Subquery(alias, nodeToPlan(query))

    case Token(laterViewToken(isOuter), selectClause :: relationClause :: Nil) =>
      val Token("TOK_SELECT",
            Token("TOK_SELEXPR", clauses) :: Nil) = selectClause

      val alias = getClause("TOK_TABALIAS", clauses).getChildren.head.asInstanceOf[ASTNode].getText

      Generate(
        nodesToGenerator(clauses),
        join = true,
        outer = isOuter.nonEmpty,
        Some(alias.toLowerCase),
        nodeToRelation(relationClause))

    /* All relations, possibly with aliases or sampling clauses. */
    case Token("TOK_TABREF", clauses) =>
      // If the last clause is not a token then it's the alias of the table.
      val (nonAliasClauses, aliasClause) =
        if (clauses.last.getText.startsWith("TOK")) {
          (clauses, None)
        } else {
          (clauses.dropRight(1), Some(clauses.last))
        }

      val (Some(tableNameParts) ::
          splitSampleClause ::
          bucketSampleClause :: Nil) = {
        getClauses(Seq("TOK_TABNAME", "TOK_TABLESPLITSAMPLE", "TOK_TABLEBUCKETSAMPLE"),
          nonAliasClauses)
      }

      val (db, tableName) =
        tableNameParts.getChildren.map{ case Token(part, Nil) => cleanIdentifier(part)} match {
          case Seq(tableOnly) => (None, tableOnly)
          case Seq(databaseName, table) => (Some(databaseName), table)
      }
      val alias = aliasClause.map { case Token(a, Nil) => cleanIdentifier(a) }
      val relation = UnresolvedRelation(db, tableName, alias)

      // Apply sampling if requested.
      (bucketSampleClause orElse splitSampleClause).map {
        case Token("TOK_TABLESPLITSAMPLE",
               Token("TOK_ROWCOUNT", Nil) ::
               Token(count, Nil) :: Nil) =>
          Limit(Literal(count.toInt), relation)
        case Token("TOK_TABLESPLITSAMPLE",
               Token("TOK_PERCENT", Nil) ::
               Token(fraction, Nil) :: Nil) =>
          Sample(fraction.toDouble, withReplacement = false, (math.random * 1000).toInt, relation)
        case Token("TOK_TABLEBUCKETSAMPLE",
               Token(numerator, Nil) ::
               Token(denominator, Nil) :: Nil) =>
          val fraction = numerator.toDouble / denominator.toDouble
          Sample(fraction, withReplacement = false, (math.random * 1000).toInt, relation)
        case a: ASTNode =>
          throw new NotImplementedError(
            s"""No parse rules for sampling clause: ${a.getType}, text: ${a.getText} :
           |${dumpTree(a).toString}" +
         """.stripMargin)
      }.getOrElse(relation)

    case Token("TOK_UNIQUEJOIN", joinArgs) =>
      val tableOrdinals =
        joinArgs.zipWithIndex.filter {
          case (arg, i) => arg.getText == "TOK_TABREF"
        }.map(_._2)

      val isPreserved = tableOrdinals.map(i => (i - 1 < 0) || joinArgs(i - 1).getText == "PRESERVE")
      val tables = tableOrdinals.map(i => nodeToRelation(joinArgs(i)))
      val joinExpressions = tableOrdinals.map(i => joinArgs(i + 1).getChildren.map(nodeToExpr))

      val joinConditions = joinExpressions.sliding(2).map {
        case Seq(c1, c2) =>
          val predicates = (c1, c2).zipped.map { case (e1, e2) => EqualTo(e1, e2): Expression }
          predicates.reduceLeft(And)
      }.toBuffer

      val joinType = isPreserved.sliding(2).map {
        case Seq(true, true) => FullOuter
        case Seq(true, false) => LeftOuter
        case Seq(false, true) => RightOuter
        case Seq(false, false) => Inner
      }.toBuffer

      val joinedTables = tables.reduceLeft(Join(_,_, Inner, None))

      // Must be transform down.
      val joinedResult = joinedTables transform {
        case j: Join =>
          j.copy(
            condition = Some(joinConditions.remove(joinConditions.length - 1)),
            joinType = joinType.remove(joinType.length - 1))
      }

      val groups = (0 until joinExpressions.head.size).map(i => Coalesce(joinExpressions.map(_(i))))

      // Unique join is not really the same as an outer join so we must group together results where
      // the joinExpressions are the same, taking the First of each value is only okay because the
      // user of a unique join is implicitly promising that there is only one result.
      // TODO: This doesn't actually work since [[Star]] is not a valid aggregate expression.
      // instead we should figure out how important supporting this feature is and whether it is
      // worth the number of hacks that will be required to implement it.  Namely, we need to add
      // some sort of mapped star expansion that would expand all child output row to be similarly
      // named output expressions where some aggregate expression has been applied (i.e. First).
      ??? // Aggregate(groups, Star(None, First(_)) :: Nil, joinedResult)

    case Token(allJoinTokens(joinToken),
           relation1 ::
           relation2 :: other) =>
      assert(other.size <= 1, s"Unhandled join child $other")
      val joinType = joinToken match {
        case "TOK_JOIN" => Inner
        case "TOK_RIGHTOUTERJOIN" => RightOuter
        case "TOK_LEFTOUTERJOIN" => LeftOuter
        case "TOK_FULLOUTERJOIN" => FullOuter
        case "TOK_LEFTSEMIJOIN" => LeftSemi
      }
      assert(other.size <= 1, "Unhandled join clauses.")
      Join(nodeToRelation(relation1),
        nodeToRelation(relation2),
        joinType,
        other.headOption.map(nodeToExpr))

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  def nodeToSortOrder(node: Node): SortOrder = node match {
    case Token("TOK_TABSORTCOLNAMEASC", sortExpr :: Nil) =>
      SortOrder(nodeToExpr(sortExpr), Ascending)
    case Token("TOK_TABSORTCOLNAMEDESC", sortExpr :: Nil) =>
      SortOrder(nodeToExpr(sortExpr), Descending)

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  val destinationToken = "TOK_DESTINATION|TOK_INSERT_INTO".r
  protected def nodeToDest(
      node: Node,
      query: LogicalPlan,
      overwrite: Boolean): LogicalPlan = node match {
    case Token(destinationToken(),
           Token("TOK_DIR",
             Token("TOK_TMP_FILE", Nil) :: Nil) :: Nil) =>
      query

    case Token(destinationToken(),
           Token("TOK_TAB",
              tableArgs) :: Nil) =>
      val Some(tableNameParts) :: partitionClause :: Nil =
        getClauses(Seq("TOK_TABNAME", "TOK_PARTSPEC"), tableArgs)

      val (db, tableName) = extractDbNameTableName(tableNameParts)

      val partitionKeys = partitionClause.map(_.getChildren.map {
        // Parse partitions. We also make keys case insensitive.
        case Token("TOK_PARTVAL", Token(key, Nil) :: Token(value, Nil) :: Nil) =>
          cleanIdentifier(key.toLowerCase) -> Some(PlanUtils.stripQuotes(value))
        case Token("TOK_PARTVAL", Token(key, Nil) :: Nil) =>
          cleanIdentifier(key.toLowerCase) -> None
      }.toMap).getOrElse(Map.empty)

      if (partitionKeys.values.exists(p => p.isEmpty)) {
        throw new NotImplementedError(s"Do not support INSERT INTO/OVERWRITE with" +
          s"dynamic partitioning.")
      }

      InsertIntoTable(UnresolvedRelation(db, tableName, None), partitionKeys, query, overwrite)

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }

  protected def selExprNodeToExpr(node: Node): Option[Expression] = node match {
    case Token("TOK_SELEXPR",
           e :: Nil) =>
      Some(nodeToExpr(e))

    case Token("TOK_SELEXPR",
           e :: Token(alias, Nil) :: Nil) =>
      Some(Alias(nodeToExpr(e), alias)())

    /* Hints are ignored */
    case Token("TOK_HINTLIST", _) => None

    case a: ASTNode =>
      throw new NotImplementedError(s"No parse rules for:\n ${dumpTree(a).toString} ")
  }


  protected val escapedIdentifier = "`([^`]+)`".r
  /** Strips backticks from ident if present */
  protected def cleanIdentifier(ident: String): String = ident match {
    case escapedIdentifier(i) => i
    case plainIdent => plainIdent
  }

  val numericAstTypes = Seq(
    HiveParser.Number,
    HiveParser.TinyintLiteral,
    HiveParser.SmallintLiteral,
    HiveParser.BigintLiteral)

  /* Case insensitive matches */
  val COUNT = "(?i)COUNT".r
  val AVG = "(?i)AVG".r
  val SUM = "(?i)SUM".r
  val MAX = "(?i)MAX".r
  val MIN = "(?i)MIN".r
  val UPPER = "(?i)UPPER".r
  val LOWER = "(?i)LOWER".r
  val RAND = "(?i)RAND".r
  val AND = "(?i)AND".r
  val OR = "(?i)OR".r
  val NOT = "(?i)NOT".r
  val TRUE = "(?i)TRUE".r
  val FALSE = "(?i)FALSE".r
  val LIKE = "(?i)LIKE".r
  val RLIKE = "(?i)RLIKE".r
  val REGEXP = "(?i)REGEXP".r
  val IN = "(?i)IN".r
  val DIV = "(?i)DIV".r
  val BETWEEN = "(?i)BETWEEN".r
  val WHEN = "(?i)WHEN".r
  val CASE = "(?i)CASE".r

  protected def nodeToExpr(node: Node): Expression = node match {
    /* Attribute References */
    case Token("TOK_TABLE_OR_COL",
           Token(name, Nil) :: Nil) =>
      UnresolvedAttribute(cleanIdentifier(name))
    case Token(".", qualifier :: Token(attr, Nil) :: Nil) =>
      nodeToExpr(qualifier) match {
        case UnresolvedAttribute(qualifierName) =>
          UnresolvedAttribute(qualifierName + "." + cleanIdentifier(attr))
        // The precidence for . seems to be wrong, so [] binds tighter an we need to go inside to
        // find the underlying attribute references.
        case GetItem(UnresolvedAttribute(qualifierName), ordinal) =>
          GetItem(UnresolvedAttribute(qualifierName + "." + cleanIdentifier(attr)), ordinal)
      }

    /* Stars (*) */
    case Token("TOK_ALLCOLREF", Nil) => Star(None)
    // The format of dbName.tableName.* cannot be parsed by HiveParser. TOK_TABNAME will only
    // has a single child which is tableName.
    case Token("TOK_ALLCOLREF", Token("TOK_TABNAME", Token(name, Nil) :: Nil) :: Nil) =>
      Star(Some(name))

    /* Aggregate Functions */
    case Token("TOK_FUNCTION", Token(AVG(), Nil) :: arg :: Nil) => Average(nodeToExpr(arg))
    case Token("TOK_FUNCTION", Token(COUNT(), Nil) :: arg :: Nil) => Count(nodeToExpr(arg))
    case Token("TOK_FUNCTIONSTAR", Token(COUNT(), Nil) :: Nil) => Count(Literal(1))
    case Token("TOK_FUNCTIONDI", Token(COUNT(), Nil) :: args) => CountDistinct(args.map(nodeToExpr))
    case Token("TOK_FUNCTION", Token(SUM(), Nil) :: arg :: Nil) => Sum(nodeToExpr(arg))
    case Token("TOK_FUNCTIONDI", Token(SUM(), Nil) :: arg :: Nil) => SumDistinct(nodeToExpr(arg))
    case Token("TOK_FUNCTION", Token(MAX(), Nil) :: arg :: Nil) => Max(nodeToExpr(arg))
    case Token("TOK_FUNCTION", Token(MIN(), Nil) :: arg :: Nil) => Min(nodeToExpr(arg))

    /* System functions about string operations */
    case Token("TOK_FUNCTION", Token(UPPER(), Nil) :: arg :: Nil) => Upper(nodeToExpr(arg))
    case Token("TOK_FUNCTION", Token(LOWER(), Nil) :: arg :: Nil) => Lower(nodeToExpr(arg))

    /* Casts */
    case Token("TOK_FUNCTION", Token("TOK_STRING", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), StringType)
    case Token("TOK_FUNCTION", Token("TOK_VARCHAR", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), StringType)
    case Token("TOK_FUNCTION", Token("TOK_INT", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), IntegerType)
    case Token("TOK_FUNCTION", Token("TOK_BIGINT", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), LongType)
    case Token("TOK_FUNCTION", Token("TOK_FLOAT", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), FloatType)
    case Token("TOK_FUNCTION", Token("TOK_DOUBLE", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), DoubleType)
    case Token("TOK_FUNCTION", Token("TOK_SMALLINT", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), ShortType)
    case Token("TOK_FUNCTION", Token("TOK_TINYINT", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), ByteType)
    case Token("TOK_FUNCTION", Token("TOK_BINARY", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), BinaryType)
    case Token("TOK_FUNCTION", Token("TOK_BOOLEAN", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), BooleanType)
    case Token("TOK_FUNCTION", Token("TOK_DECIMAL", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), DecimalType)
    case Token("TOK_FUNCTION", Token("TOK_TIMESTAMP", Nil) :: arg :: Nil) =>
      Cast(nodeToExpr(arg), TimestampType)

    /* Arithmetic */
    case Token("-", child :: Nil) => UnaryMinus(nodeToExpr(child))
    case Token("+", left :: right:: Nil) => Add(nodeToExpr(left), nodeToExpr(right))
    case Token("-", left :: right:: Nil) => Subtract(nodeToExpr(left), nodeToExpr(right))
    case Token("*", left :: right:: Nil) => Multiply(nodeToExpr(left), nodeToExpr(right))
    case Token("/", left :: right:: Nil) => Divide(nodeToExpr(left), nodeToExpr(right))
    case Token(DIV(), left :: right:: Nil) => Divide(nodeToExpr(left), nodeToExpr(right))
    case Token("%", left :: right:: Nil) => Remainder(nodeToExpr(left), nodeToExpr(right))

    /* Comparisons */
    case Token("=", left :: right:: Nil) => EqualTo(nodeToExpr(left), nodeToExpr(right))
    case Token("!=", left :: right:: Nil) => Not(EqualTo(nodeToExpr(left), nodeToExpr(right)))
    case Token("<>", left :: right:: Nil) => Not(EqualTo(nodeToExpr(left), nodeToExpr(right)))
    case Token(">", left :: right:: Nil) => GreaterThan(nodeToExpr(left), nodeToExpr(right))
    case Token(">=", left :: right:: Nil) => GreaterThanOrEqual(nodeToExpr(left), nodeToExpr(right))
    case Token("<", left :: right:: Nil) => LessThan(nodeToExpr(left), nodeToExpr(right))
    case Token("<=", left :: right:: Nil) => LessThanOrEqual(nodeToExpr(left), nodeToExpr(right))
    case Token(LIKE(), left :: right:: Nil) => Like(nodeToExpr(left), nodeToExpr(right))
    case Token(RLIKE(), left :: right:: Nil) => RLike(nodeToExpr(left), nodeToExpr(right))
    case Token(REGEXP(), left :: right:: Nil) => RLike(nodeToExpr(left), nodeToExpr(right))
    case Token("TOK_FUNCTION", Token("TOK_ISNOTNULL", Nil) :: child :: Nil) =>
      IsNotNull(nodeToExpr(child))
    case Token("TOK_FUNCTION", Token("TOK_ISNULL", Nil) :: child :: Nil) =>
      IsNull(nodeToExpr(child))
    case Token("TOK_FUNCTION", Token(IN(), Nil) :: value :: list) =>
      In(nodeToExpr(value), list.map(nodeToExpr))
    case Token("TOK_FUNCTION",
           Token(BETWEEN(), Nil) ::
           Token("KW_FALSE", Nil) ::
           target ::
           minValue ::
           maxValue :: Nil) =>

      val targetExpression = nodeToExpr(target)
      And(
        GreaterThanOrEqual(targetExpression, nodeToExpr(minValue)),
        LessThanOrEqual(targetExpression, nodeToExpr(maxValue)))

    /* Boolean Logic */
    case Token(AND(), left :: right:: Nil) => And(nodeToExpr(left), nodeToExpr(right))
    case Token(OR(), left :: right:: Nil) => Or(nodeToExpr(left), nodeToExpr(right))
    case Token(NOT(), child :: Nil) => Not(nodeToExpr(child))

    /* Case statements */
    case Token("TOK_FUNCTION", Token(WHEN(), Nil) :: branches) =>
      CaseWhen(branches.map(nodeToExpr))
    case Token("TOK_FUNCTION", Token(CASE(), Nil) :: branches) =>
      val transformed = branches.drop(1).sliding(2, 2).map {
        case Seq(condVal, value) =>
          // FIXME (SPARK-2155): the key will get evaluated for multiple times in CaseWhen's eval().
          // Hence effectful / non-deterministic key expressions are *not* supported at the moment.
          // We should consider adding new Expressions to get around this.
          Seq(EqualTo(nodeToExpr(branches(0)), nodeToExpr(condVal)),
              nodeToExpr(value))
        case Seq(elseVal) => Seq(nodeToExpr(elseVal))
      }.toSeq.reduce(_ ++ _)
      CaseWhen(transformed)

    /* Complex datatype manipulation */
    case Token("[", child :: ordinal :: Nil) =>
      GetItem(nodeToExpr(child), nodeToExpr(ordinal))

    /* Other functions */
    case Token("TOK_FUNCTION", Token(RAND(), Nil) :: Nil) => Rand

    /* UDFs - Must be last otherwise will preempt built in functions */
    case Token("TOK_FUNCTION", Token(name, Nil) :: args) =>
      UnresolvedFunction(name, args.map(nodeToExpr))
    case Token("TOK_FUNCTIONSTAR", Token(name, Nil) :: args) =>
      UnresolvedFunction(name, Star(None) :: Nil)

    /* Literals */
    case Token("TOK_NULL", Nil) => Literal(null, NullType)
    case Token(TRUE(), Nil) => Literal(true, BooleanType)
    case Token(FALSE(), Nil) => Literal(false, BooleanType)
    case Token("TOK_STRINGLITERALSEQUENCE", strings) =>
      Literal(strings.map(s => BaseSemanticAnalyzer.unescapeSQLString(s.getText)).mkString)

    // This code is adapted from
    // /ql/src/java/org/apache/hadoop/hive/ql/parse/TypeCheckProcFactory.java#L223
    case ast: ASTNode if numericAstTypes contains ast.getType =>
      var v: Literal = null
      try {
        if (ast.getText.endsWith("L")) {
          // Literal bigint.
          v = Literal(ast.getText.substring(0, ast.getText.length() - 1).toLong, LongType)
        } else if (ast.getText.endsWith("S")) {
          // Literal smallint.
          v = Literal(ast.getText.substring(0, ast.getText.length() - 1).toShort, ShortType)
        } else if (ast.getText.endsWith("Y")) {
          // Literal tinyint.
          v = Literal(ast.getText.substring(0, ast.getText.length() - 1).toByte, ByteType)
        } else if (ast.getText.endsWith("BD")) {
          // Literal decimal
          val strVal = ast.getText.substring(0, ast.getText.length() - 2)
          BigDecimal(strVal)
        } else {
          v = Literal(ast.getText.toDouble, DoubleType)
          v = Literal(ast.getText.toLong, LongType)
          v = Literal(ast.getText.toInt, IntegerType)
        }
      } catch {
        case nfe: NumberFormatException => // Do nothing
      }

      if (v == null) {
        sys.error(s"Failed to parse number ${ast.getText}")
      } else {
        v
      }

    case ast: ASTNode if ast.getType == HiveParser.StringLiteral =>
      Literal(BaseSemanticAnalyzer.unescapeSQLString(ast.getText))

    case a: ASTNode =>
      throw new NotImplementedError(
        s"""No parse rules for ASTNode type: ${a.getType}, text: ${a.getText} :
           |${dumpTree(a).toString}" +
         """.stripMargin)
  }


  val explode = "(?i)explode".r
  def nodesToGenerator(nodes: Seq[Node]): Generator = {
    val function = nodes.head

    val attributes = nodes.flatMap {
      case Token(a, Nil) => a.toLowerCase :: Nil
      case _ => Nil
    }

    function match {
      case Token("TOK_FUNCTION", Token(explode(), Nil) :: child :: Nil) =>
        Explode(attributes, nodeToExpr(child))

      case Token("TOK_FUNCTION", Token(functionName, Nil) :: children) =>
        HiveGenericUdtf(functionName, attributes, children.map(nodeToExpr))

      case a: ASTNode =>
        throw new NotImplementedError(
          s"""No parse rules for ASTNode type: ${a.getType}, text: ${a.getText}, tree:
             |${dumpTree(a).toString}
           """.stripMargin)
    }
  }

  def dumpTree(node: Node, builder: StringBuilder = new StringBuilder, indent: Int = 0)
    : StringBuilder = {
    node match {
      case a: ASTNode => builder.append(("  " * indent) + a.getText + "\n")
      case other => sys.error(s"Non ASTNode encountered: $other")
    }

    Option(node.getChildren).map(_.toList).getOrElse(Nil).foreach(dumpTree(_, builder, indent + 1))
    builder
  }
}
