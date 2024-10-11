package net.sansa_stack.inference.rules.plan

import java.util

import scala.jdk.CollectionConverters._

import org.apache.calcite.DataContext
import org.apache.calcite.config.CalciteConnectionConfig
import org.apache.calcite.linq4j.{Enumerable, Linq4j}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelProtoDataType}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.Schema.TableType
import org.apache.calcite.schema._
import org.apache.calcite.sql.{SqlCall, SqlNode}
import org.apache.calcite.sql.`type`.SqlTypeName

/**
  * @author Lorenz Buehmann
  */
class TriplesTableFactory extends TableFactory[Table] {
  override def create(
                       schema: SchemaPlus,
                       name: String,
                       operand: util.Map[String, AnyRef],
                       rowType: RelDataType): Table = {

    val rows: List[Array[AnyRef]] = List(
      Array("s1", "p1", "o1")
    )
    new TriplesTable(rows)

  }

  class TriplesTable(val rows: List[Array[AnyRef]]) extends ScannableTable with FilterableTable with ProjectableFilterableTable {

    val protoRowType = new RelProtoDataType() {
      override def apply(a0: RelDataTypeFactory): RelDataType = {
        a0.builder()
          .add("s", SqlTypeName.VARCHAR, 200)
          .add("p", SqlTypeName.VARCHAR, 200)
          .add("o", SqlTypeName.VARCHAR, 200)
          .build()
      }
    }

    override def scan(root: DataContext): Enumerable[Array[AnyRef]] = Linq4j.asEnumerable(rows.asJava)

    override def scan(root: DataContext, filters: util.List[RexNode]): Enumerable[Array[AnyRef]] = Linq4j.asEnumerable(rows.asJava)

    override def scan(root: DataContext, filters: util.List[RexNode], projects: Array[Int]): Enumerable[Array[AnyRef]] = Linq4j.asEnumerable(rows.asJava)

    override def getStatistic: Statistic = Statistics.UNKNOWN

    override def getJdbcTableType: TableType = Schema.TableType.TABLE

    override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = protoRowType.apply(typeFactory)

    override def isRolledUp(s: String): Boolean = false

    override def rolledUpColumnValidInsideAgg(s: String, sqlCall: SqlCall, sqlNode: SqlNode,
                                              calciteConnectionConfig: CalciteConnectionConfig): Boolean = false
  }

}
