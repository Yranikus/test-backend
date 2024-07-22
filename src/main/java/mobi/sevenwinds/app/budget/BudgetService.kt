package mobi.sevenwinds.app.budget

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import mobi.sevenwinds.app.author.AuthorEntity
import mobi.sevenwinds.app.author.AuthorTable
import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

object BudgetService {

    class ILikeOp(expr1: Expression<*>, expr2: Expression<*>) : ComparisonOp(expr1, expr2, "ILIKE")

    infix fun <T : String?> ExpressionWithColumnType<T>.ilike(pattern: String): Op<Boolean> =
        ILikeOp(this, QueryParameter("%$pattern%", columnType))

    suspend fun addRecord(body: BudgetRecordRequest): BudgetRecordResponse = withContext(Dispatchers.IO) {
        transaction {
            val entity = BudgetEntity.new {
                this.year = body.year
                this.month = body.month
                this.amount = body.amount
                this.type = body.type
                this.author = if (body.authorId != null)
                    AuthorEntity.findById(body.authorId)
                else null
            }

            return@transaction entity.toResponse()
        }
    }

    suspend fun getYearStats(param: BudgetYearParam): BudgetYearStatsResponse = withContext(Dispatchers.IO) {
        transaction {
            val query = BudgetTable.leftJoin(AuthorTable)
                .select {
                    BudgetTable.year eq param.year and
                            if (!param.name.isNullOrEmpty())
                                AuthorTable.fullName ilike param.name
                            else Op.TRUE

                }

            val total = query.count()

            query
                .orderBy(BudgetTable.month, SortOrder.ASC)
                .orderBy(BudgetTable.amount, SortOrder.DESC)
                .limit(param.limit, param.offset)

            val data = BudgetEntity.wrapRows(query).map { it.toResponse() }

            val sumByType =
                BudgetTable
                    .slice(BudgetTable.type, BudgetTable.amount.sum())
                    .select { BudgetTable.year eq param.year }.groupBy(BudgetTable.type)
                    .associate { resultRow ->
                        Pair(
                            resultRow[BudgetTable.type].name,
                            resultRow[BudgetTable.amount.sum()] ?: 0
                        )
                    }

            return@transaction BudgetYearStatsResponse(
                total = total,
                totalByType = sumByType,
                items = data
            )
        }
    }
}