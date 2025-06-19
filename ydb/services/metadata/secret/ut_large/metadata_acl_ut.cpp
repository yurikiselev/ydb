#include <util/string/join.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NActors;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace NSchemeShardUT_Private {

Y_UNIT_TEST_SUITE(MetadataSecretAcl) {
    void ProvideUseGrant(const TString& user, const TString& object, NYdb::NQuery::TQueryClient& client) {
        const auto query = "GRANT USE ON `" + object + "` TO `" + user + "`;";
        const auto it = client.ExecuteQuery(
                                    query,
                                    NYdb::NQuery::TTxControl::NoTx(),
                                    NYdb::NQuery::TExecuteQuerySettings())
                            .ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        Cerr << "zzz: USE grant is provided to user `" << user << "` for object `" << object << "`\n";
    }

    void CheckUseGrant(const TString& user, NKikimr::NKqp::TKikimrRunner& kikimr) {
        auto driverConfig = TDriverConfig()
                                .SetEndpoint(kikimr.GetEndpoint())
                                .SetAuthToken(user);
        auto driver = TDriver(driverConfig);
        auto client = NYdb::NQuery::TQueryClient(driver);
        const auto queryCreateTable = "CREATE TABLE `/Root/table` (a Uint64, b Uint64, PRIMARY KEY (a, b));";
        NKikimr::NKqp::AssertSuccessResult(client.ExecuteQuery(
                                                        queryCreateTable,
                                                        NYdb::NQuery::TTxControl::NoTx(),
                                                        NYdb::NQuery::TExecuteQuerySettings())
                                                .ExtractValueSync());

        const auto queryReadTable = "SELECT * FROM `/Root/table`;";
        NKikimr::NKqp::AssertSuccessResult(client.ExecuteQuery(
                                                        queryReadTable,
                                                        NYdb::NQuery::TTxControl::NoTx(),
                                                        NYdb::NQuery::TExecuteQuerySettings())
                                                .ExtractValueSync());

        Cerr << "zzz: USE grant is checked for for user `" << user << "`\n";
    }

    void CreateSecret(NYdb::NQuery::TQueryClient& client) {
        const auto query = "CREATE OBJECT `MySecretName` (TYPE SECRET) WITH value=`MySecretData`;";
        auto it = client.ExecuteQuery(
                            query,
                            NYdb::NQuery::TTxControl::NoTx(),
                            NYdb::NQuery::TExecuteQuerySettings())
                        .ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        Cerr << "zzz: secret creation is done\n";
    }

    void ReadSecrets(const TString& user, const bool expectedSuccess, NKikimr::NKqp::TKikimrRunner& kikimr) {
        auto driverConfig = TDriverConfig()
                                .SetEndpoint(kikimr.GetEndpoint())
                                .SetAuthToken(user);
        auto driver = TDriver(driverConfig);
        auto client = NYdb::NQuery::TQueryClient(driver);
        const auto query = "SELECT * from `/Root/.metadata/secrets/values`;";
        if (expectedSuccess) {
            NKikimr::NKqp::AssertSuccessResult(client.ExecuteQuery(
                                                            query,
                                                            NYdb::NQuery::TTxControl::NoTx(),
                                                            NYdb::NQuery::TExecuteQuerySettings())
                                                    .ExtractValueSync());
            Cerr << "zzz: select for user `" << user << "`has succeded\n";
        } else {
            auto result = client.ExecuteQuery(
                                    query,
                                    NYdb::NQuery::TTxControl::NoTx(),
                                    NYdb::NQuery::TExecuteQuerySettings())
                                .ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            Cerr << "zzz: select for user `" << user << "`has failed\n";
        }

        driver.Stop(true);
    }

    void TestOneRun() {
        auto settings = NKikimr::NKqp::TKikimrSettings().SetWithSampleTables(false);
        NKikimr::NKqp::TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        ProvideUseGrant("root@builtin", "/Root", client);
        CheckUseGrant("root@builtin", kikimr);
        CreateSecret(client);
        ReadSecrets("metadata@system", /* expectedSuccess */ true, kikimr);
        ReadSecrets("root@builtin", /* expectedSuccess */ false, kikimr);
    }

    Y_UNIT_TEST(OneRun) {
        TestOneRun();
    }

    Y_UNIT_TEST(ManyRuns) {
        for (int run = 0; run < 500; ++run) {
            TestOneRun();
        }
    }

} // Y_UNIT_TEST_SUITE(MetadataSecretAcl)
} // namespace NSchemeShardUT_Private
