package mobi.sevenwinds.app.budget

import io.restassured.RestAssured
import mobi.sevenwinds.common.ServerTest
import mobi.sevenwinds.common.jsonBody
import mobi.sevenwinds.common.toResponse
import org.assertj.core.internal.bytebuddy.utility.RandomString
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.Assert
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.junit.runners.Parameterized.Parameters

class BudgetApiKtTest : ServerTest() {

    @BeforeEach
    internal fun setUp() {
        transaction { BudgetTable.deleteAll() }
    }

    @Test
    fun testBudgetPagination() {
        addRecord(BudgetRecordRequest(2020, 5, 10, BudgetType.Приход))
        addRecord(BudgetRecordRequest(2020, 5, 5, BudgetType.Приход))
        addRecord(BudgetRecordRequest(2020, 5, 20, BudgetType.Приход))
        addRecord(BudgetRecordRequest(2020, 5, 30, BudgetType.Приход))
        addRecord(BudgetRecordRequest(2020, 5, 40, BudgetType.Приход))
        addRecord(BudgetRecordRequest(2030, 1, 1, BudgetType.Расход))

        RestAssured.given()
            .queryParam("limit", 3)
            .queryParam("offset", 1)
            .get("/budget/year/2020/stats")
            .toResponse<BudgetYearStatsResponse>().let { response ->
                println("${response.total} / ${response.items} / ${response.totalByType}")

                Assert.assertEquals(5, response.total)
                Assert.assertEquals(3, response.items.size)
                Assert.assertEquals(105, response.totalByType[BudgetType.Приход.name])
            }
    }

    @Test
    fun testStatsSortOrder() {
        addRecord(BudgetRecordRequest(2020, 5, 100, BudgetType.Приход))
        addRecord(BudgetRecordRequest(2020, 1, 5, BudgetType.Приход))
        addRecord(BudgetRecordRequest(2020, 5, 50, BudgetType.Приход))
        addRecord(BudgetRecordRequest(2020, 1, 30, BudgetType.Приход))
        addRecord(BudgetRecordRequest(2020, 5, 400, BudgetType.Приход))

        // expected sort order - month ascending, amount descending

        RestAssured.given()
            .get("/budget/year/2020/stats?limit=100&offset=0")
            .toResponse<BudgetYearStatsResponse>().let { response ->
                println(response.items)

                Assert.assertEquals(30, response.items[0].amount)
                Assert.assertEquals(5, response.items[1].amount)
                Assert.assertEquals(400, response.items[2].amount)
                Assert.assertEquals(100, response.items[3].amount)
                Assert.assertEquals(50, response.items[4].amount)
            }
    }

    @Test
    fun testInvalidMonthValues() {
        RestAssured.given()
            .jsonBody(BudgetRecordRequest(2020, -5, 5, BudgetType.Приход))
            .post("/budget/add")
            .then().statusCode(400)

        RestAssured.given()
            .jsonBody(BudgetRecordRequest(2020, 15, 5, BudgetType.Приход))
            .post("/budget/add")
            .then().statusCode(400)
    }

    @RepeatedTest(5)
    fun testGetBudgetWithAuthorName(){
        val firstName = RandomString.make(4);
        val lastName = RandomString.make(6);
        val middleName = RandomString.make(3)
        val fullName = "$lastName $firstName $middleName"

        val authorId = addAuthor(fullName)

        println(authorId)

        addRecord(BudgetRecordRequest(2020, 5, 100, BudgetType.Приход, authorId))

        RestAssured.given().queryParam("limit", 100).queryParams("offset", 0).queryParam("name", firstName.toUpperCase())
            .get("/budget/year/2020/stats")
            .toResponse<BudgetYearStatsResponse>().let{ response ->
                Assert.assertEquals(response.items[0].fullName, fullName)
            }

        RestAssured.given().queryParam("limit", 100).queryParams("offset", 0).queryParam("name", lastName.substring(2,6).toLowerCase())
            .get("/budget/year/2020/stats")
            .toResponse<BudgetYearStatsResponse>().let{ response ->
                Assert.assertEquals(response.items[0].fullName, fullName)
            }

        RestAssured.given().queryParam("limit", 100).queryParams("offset", 0).queryParam("name", "$lastName $firstName")
            .get("/budget/year/2020/stats")
            .toResponse<BudgetYearStatsResponse>().let{ response ->
                Assert.assertEquals(response.items[0].fullName, fullName)
            }

        RestAssured.given().queryParam("limit", 100).queryParams("offset", 0).queryParam("name", "w3e12313123123213213j12j3ggashfgasf")
            .get("/budget/year/2020/stats")
            .toResponse<BudgetYearStatsResponse>().let{ response ->
                Assert.assertEquals(response.total, 0)
            }

    }

    private fun addRecord(record: BudgetRecordRequest) {
        RestAssured.given()
            .jsonBody(record)
            .post("/budget/add")
            .toResponse<BudgetRecordResponse>().let { response ->
                Assert.assertEquals(record.type, response.type)
                Assert.assertEquals(record.year, response.year)
                Assert.assertEquals(record.amount, response.amount)
                Assert.assertEquals(record.month, response.month)
            }
    }

    private fun addAuthor(fullName: String) : Int{
        return RestAssured
            .given()
            .jsonBody(fullName)
            .post("/author/add")
            .toResponse<Int>()
    }
}