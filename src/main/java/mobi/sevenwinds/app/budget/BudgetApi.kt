package mobi.sevenwinds.app.budget

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.papsign.ktor.openapigen.annotations.parameters.PathParam
import com.papsign.ktor.openapigen.annotations.parameters.QueryParam
import com.papsign.ktor.openapigen.annotations.type.number.integer.max.Max
import com.papsign.ktor.openapigen.annotations.type.number.integer.min.Min
import com.papsign.ktor.openapigen.route.info
import com.papsign.ktor.openapigen.route.path.normal.NormalOpenAPIRoute
import com.papsign.ktor.openapigen.route.path.normal.get
import com.papsign.ktor.openapigen.route.path.normal.post
import com.papsign.ktor.openapigen.route.response.respond
import com.papsign.ktor.openapigen.route.route
import com.typesafe.config.Optional
import mobi.sevenwinds.CustomDateTimeDeserializer
import org.joda.time.DateTime

fun NormalOpenAPIRoute.budget() {
    route("/budget") {
        route("/add").post<Unit, BudgetRecordResponse, BudgetRecordRequest>(info("Добавить запись")) { param, body ->
            respond(BudgetService.addRecord(body))
        }

        route("/year/{year}/stats") {
            get<BudgetYearParam, BudgetYearStatsResponse>(info("Получить статистику за год")) { param ->
                respond(BudgetService.getYearStats(param))
            }
        }
    }
}

data class BudgetRecordRequest(
    @Min(1900) val year: Int,
    @Min(1) @Max(12) val month: Int,
    @Min(1) val amount: Int,
    val type: BudgetType,
    @Min(1) @Optional  val authorId: Int? = null
)

data class BudgetRecordResponse(
    val year: Int,
    val month: Int,
    val amount: Int,
    val type: BudgetType,
    val fullName: String? = null,
    @JsonDeserialize(using = CustomDateTimeDeserializer::class)
    val dateCreated: DateTime? = null
)

data class BudgetYearParam(
    @PathParam("Год") val year: Int,
    @QueryParam("Лимит пагинации") val limit: Int,
    @QueryParam("Смещение пагинации") val offset: Int,
    @QueryParam("Фильтр по ФИО") @Optional val name: String?
)

class BudgetYearStatsResponse(
    val total: Int,
    val totalByType: Map<String, Int>,
    val items: List<BudgetRecordResponse>
)

enum class BudgetType {
    Приход, Расход
}