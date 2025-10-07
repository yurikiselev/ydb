#include "kqp_federated_query_actors.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/simple/services.h>

namespace NKikimr::NKqp {

namespace {
    constexpr auto TestTimeout = TDuration::Seconds(10);

    void CreateSchemaSecret(const TString& secretName, const TString& secretValue, NYdb::NTable::TSession& session) {
        const auto query = "CREATE SECRET `" + secretName + "` WITH (value = \"" + secretValue + "\");";
        const auto queryResult = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, queryResult.GetStatus(), queryResult.GetIssues().ToString());
    }

    void AlterSchemaSecret(const TString& secretName, const TString& secretValue, NYdb::NTable::TSession& session) {
        const auto query = "ALTER  SECRET `" + secretName + "` WITH (value = \"" + secretValue + "\");";
        const auto queryResult = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, queryResult.GetStatus(), queryResult.GetIssues().ToString());
    }

    void DropSchemaSecret(const TString& secretName, NYdb::NTable::TSession& session) {
        const auto query = "DROP  SECRET `" + secretName + "`;";
        const auto queryResult = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, queryResult.GetStatus(), queryResult.GetIssues().ToString());
    }

    NActors::TActorId SchemaSecretServiceId(auto& runtime) {
        return MakeKqpDescribeSchemaSecretServiceId(runtime.GetNodeId());
    }

    TIntrusiveConstPtr<NACLib::TUserToken> GetUserToken(
        const TString& userSid = "",
        const TVector<TString>& groupSids = {}
    ) {
        if (userSid.empty() && groupSids.empty()) {
            return nullptr;
        }
        return new NACLib::TUserToken(userSid, groupSids);
    }

    NThreading::TPromise<TEvDescribeSecretsResponse::TDescription>
    ResolveSecret(
        const TVector<TString>& secretNames,
        TKikimrRunner& kikimr,
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr
    ) {
        auto result = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
        const auto evResolveSecret = new TEvResolveSecretWithPromise(userToken, "/Root", secretNames, result);
        auto actorSystem = kikimr.GetTestServer().GetRuntime()->GetActorSystem(0);
        actorSystem->Send(SchemaSecretServiceId(*kikimr.GetTestServer().GetRuntime()), evResolveSecret);
        return result;
    }

    NThreading::TPromise<TEvDescribeSecretsResponse::TDescription>
    ResolveSecret(
        const TString& secretName,
        TKikimrRunner& kikimr,
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr
    ) {
        return ResolveSecret(TVector<TString>{secretName}, kikimr, userToken);
    }

    TEvResolveSecretResponse::TPtr ResolveSecretWithEvent(
        const TVector<TString>& secretNames,
        TKikimrRunner& kikimr,
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr
    ) {
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();
        auto ev = MakeHolder<TEvResolveSecretWithEvent>(userToken, "/Root", secretNames);

        runtime.Send(new IEventHandle(SchemaSecretServiceId(runtime), edgeActor, ev.Release()));

        const auto reply = runtime.GrabEdgeEvent<TEvResolveSecretResponse>(edgeActor, TestTimeout);
        UNIT_ASSERT_C(reply, "Resolve secret response is empty");

        return reply;
    }

    template <class TDescription>
    void AssertBadRequest(const TDescription& description, const TString& errMessage) {
        UNIT_ASSERT_VALUES_EQUAL(Ydb::StatusIds::BAD_REQUEST, description.Status);
        UNIT_ASSERT_VALUES_EQUAL(errMessage, description.Issues.ToString());
    }

    void AssertBadRequest(
        const bool describeWithEvent,
        const TVector<TString>& secretNames,
        const TString& errMessage,
        TKikimrRunner& kikimr,
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr
    ) {
        if (describeWithEvent) {
            auto event = ResolveSecretWithEvent(secretNames, kikimr, userToken);
            AssertBadRequest(event->Get()->Description, errMessage);
        } else {
            auto promise = ResolveSecret(secretNames, kikimr, userToken);
            AssertBadRequest(promise.GetFuture().GetValueSync(), errMessage);
        }
    }

    void AssertBadRequest(
        const bool describeWithEvent,
        const TString& secretName,
        const TString& errMessage,
        TKikimrRunner& kikimr,
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr
    ) {
        AssertBadRequest(describeWithEvent, TVector<TString>{ secretName }, errMessage, kikimr, userToken);
    }

    void AssertSecretValues(
        const bool describeWithEvent,
        const TVector<TString> secretNames,
        const TVector<TString>& secretValues,
        TKikimrRunner& kikimr,
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr
    ) {
        Y_ENSURE(secretValues.size() == secretNames.size(), "Secret values and names must have the same size");

        if (describeWithEvent) {
            const auto event = ResolveSecretWithEvent(secretNames, kikimr, userToken);
            UNIT_ASSERT_VALUES_EQUAL(secretValues.size(), event->Get()->Description.SecretValues.size());
            for (size_t i = 0; i < secretValues.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(secretValues[i], event->Get()->Description.SecretValues[i]);
            }
        } else {
            const auto promise = ResolveSecret(secretNames, kikimr, userToken);
            UNIT_ASSERT_VALUES_EQUAL(secretValues.size(), promise.GetFuture().GetValueSync().SecretValues.size());
            for (size_t i = 0; i < secretValues.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(secretValues[i], promise.GetFuture().GetValueSync().SecretValues[i]);
            }
        }
    }

    void AssertSecretValue(
        const bool describeWithEvent,
        const TString& secretName,
        const TString& secretValue,
        TKikimrRunner& kikimr,
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr
    ) {
        AssertSecretValues(describeWithEvent, { secretName }, { secretValue }, kikimr, userToken);
    }
}

Y_UNIT_TEST_SUITE(DescribeSchemaSecretsService) {
    Y_UNIT_TEST_TWIN(GetNewValue, DescribeWithEvent) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        const TString secretName = "/Root/secret-name";
        const TString secretValue = "secret-value";
        CreateSchemaSecret(secretName, secretValue, session);

        for (int i = 0; i < 3; ++i) {
            AssertSecretValue(DescribeWithEvent, secretName, secretValue, kikimr);
        }
    }

    Y_UNIT_TEST_TWIN(GetUpdatedValue, DescribeWithEvent) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        const TString secretName = "/Root/secret-name";
        const TString secretValue = "secret-value";
        CreateSchemaSecret(secretName, secretValue, session);

        AssertSecretValue(DescribeWithEvent, secretName, secretValue, kikimr);

        for (int i = 0; i < 3; ++i) {
            const TString newSecretValue = secretValue + "-" + ToString(i);
            AlterSchemaSecret(secretName, newSecretValue, session);

            AssertSecretValue(DescribeWithEvent, secretName, newSecretValue, kikimr);
        }
    }

    Y_UNIT_TEST_TWIN(GetUnexistingValue, DescribeWithEvent) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        AssertBadRequest(
            DescribeWithEvent,
            "/Root/secret-not-exist",
            "<main>: Error: secret `/Root/secret-not-exist` not found\n",
            kikimr
        );
    }

    Y_UNIT_TEST_TWIN(GetDroppedValue, DescribeWithEvent) {
        class TTestSecretUpdateListener : public TDescribeSchemaSecretsService::ISecretUpdateListener {
        public:
            NThreading::TPromise<TString> DeletionPromise = NThreading::NewPromise<TString>();

        public:
            void HandleNotifyDelete(const TString& secretName) override {
                Y_ENSURE(!DeletionPromise.HasValue()); // only one call of HandleNotifyDelete is expected
                DeletionPromise.SetValue(secretName);
            }
        };

        class TTestDescribeSchemaSecretsServiceFactory : public IDescribeSchemaSecretsServiceFactory {
        public:
            TTestDescribeSchemaSecretsServiceFactory(
                TDescribeSchemaSecretsService::ISecretUpdateListener* secretUpdateListener
            )
                : SecretUpdateListener(secretUpdateListener)
            {
            }

            NActors::IActor* CreateService() override {
                auto* service = new TDescribeSchemaSecretsService();
                service->SetSecretUpdateListener(SecretUpdateListener);
                return service;
            }

        private:
            TDescribeSchemaSecretsService::ISecretUpdateListener* SecretUpdateListener;
        };

        TKikimrSettings settings;
        auto secretUpdateListener = MakeHolder<TTestSecretUpdateListener>();
        auto factory = std::make_shared<TTestDescribeSchemaSecretsServiceFactory>(secretUpdateListener.Get());
        settings.SetDescribeSchemaSecretsServiceFactory(factory);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString secretName = "/Root/secret-name";
        TString secretValue = "secret-value";
        CreateSchemaSecret(secretName, secretValue, session);

        AssertSecretValue(DescribeWithEvent, secretName, secretValue, kikimr);

        DropSchemaSecret(secretName, session);
        UNIT_ASSERT_VALUES_EQUAL(secretName, secretUpdateListener->DeletionPromise.GetFuture().GetValueSync());

        AssertBadRequest(DescribeWithEvent, secretName,
            "<main>: Error: secret `/Root/secret-name` not found\n", kikimr);

        secretValue += "-updated";
        CreateSchemaSecret(secretName, secretValue, session);

        AssertSecretValue(DescribeWithEvent, secretName, secretValue, kikimr);
    }

    Y_UNIT_TEST(GetInParallelDescribeWithPromise) {
        static const int SECRETS_CNT = 5;
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto resolveAndCheckSecrets = [&](const std::vector<std::pair<TString, TString>>& secrets) {
            std::vector<NThreading::TPromise<TEvDescribeSecretsResponse::TDescription>> promises;
            for (const auto& [secretName, secretValue] : secrets) {
                promises.push_back(ResolveSecret(secretName, kikimr));
            }

            for (int i = 0; i < SECRETS_CNT; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(secrets[i].second, promises[i].GetFuture().GetValueSync().SecretValues[0]);
            }
        };

        // new values
        std::vector<std::pair<TString, TString>> secrets;
        for (int i = 0; i < SECRETS_CNT; ++i) {
            secrets.push_back({"/Root/secret-name-" + ToString(i), "secret-value-" + ToString(i)});
            CreateSchemaSecret(secrets.back().first, secrets.back().second, session);
        }
        resolveAndCheckSecrets(secrets);

        // altered values
        for (int i = 0; i < SECRETS_CNT; ++i) {
            secrets[i].second += "-new";
            AlterSchemaSecret(secrets[i].first, secrets[i].second, session);
        }
        resolveAndCheckSecrets(secrets);
    }

    Y_UNIT_TEST(GetInParallelDescribeWithEvent) {
        static const int SECRETS_CNT = 5;
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        auto resolveAndCheckSecrets = [&](const std::vector<std::pair<TString, TString>>& secrets) {
            for (ui64 i = 0; i < SECRETS_CNT; ++i) {
                auto ev = MakeHolder<TEvResolveSecretWithEvent>(
                    /* userToken */ nullptr,
                    "/Root",
                    TVector<TString>{ secrets[i].first }
                );
                runtime.Send(new IEventHandle(SchemaSecretServiceId(runtime), edgeActor, ev.Release(), 0, /* cookie */ i));
            }

            for (int i = 0; i < SECRETS_CNT; ++i) {
                const auto reply = runtime.GrabEdgeEvent<TEvResolveSecretResponse>(edgeActor, TestTimeout);
                UNIT_ASSERT_C(reply, "Resolve secret response is empty");
                UNIT_ASSERT_VALUES_EQUAL(secrets[reply->Cookie].second, reply->Get()->Description.SecretValues[0]);
            }
        };

        // new values
        std::vector<std::pair<TString, TString>> secrets;
        for (int i = 0; i < SECRETS_CNT; ++i) {
            secrets.push_back({"/Root/secret-name-" + ToString(i), "secret-value-" + ToString(i)});
            CreateSchemaSecret(secrets.back().first, secrets.back().second, session);
        }
        resolveAndCheckSecrets(secrets);

        // altered values
        for (int i = 0; i < SECRETS_CNT; ++i) {
            secrets[i].second += "-new";
            AlterSchemaSecret(secrets[i].first, secrets[i].second, session);
        }
        resolveAndCheckSecrets(secrets);
    }

    Y_UNIT_TEST_TWIN(UserGrants, DescribeWithEvent) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);

        const TString secretName = "/Root/secret-name";
        const TString secretValue = "secret-value";
        auto adminSession = kikimr.GetTableClient(NYdb::NTable::TClientSettings().AuthToken("root@builtin"))
            .CreateSession().GetValueSync().GetSession();

        CreateSchemaSecret(secretName, secretValue, adminSession);

        AssertSecretValue(DescribeWithEvent, secretName, secretValue, kikimr, GetUserToken("root@builtin"));

        // assert no grants by default
        const auto userToken = GetUserToken("user@builtin");
        AssertBadRequest(DescribeWithEvent, secretName, "<main>: Error: secret `/Root/secret-name` not found\n",
            kikimr, userToken);

        // assert grants are ok after providing them
        const auto grantResult = adminSession.ExecuteSchemeQuery(
            Sprintf("GRANT 'ydb.granular.select_row' ON `%s` TO `%s`;", secretName.data(), "user@builtin")
        ).GetValueSync();
        UNIT_ASSERT_C(grantResult.GetStatus() == NYdb::EStatus::SUCCESS, grantResult.GetIssues().ToString());
        AssertSecretValue(DescribeWithEvent, secretName, secretValue, kikimr, userToken);

        // assert no grants after revoking them
        const auto revokeResult = adminSession.ExecuteSchemeQuery(
            Sprintf("REVOKE 'ydb.granular.select_row' ON `%s` FROM `%s`;", secretName.data(), "user@builtin")
        ).GetValueSync();
        UNIT_ASSERT_C(revokeResult.GetStatus() == NYdb::EStatus::SUCCESS, grantResult.GetIssues().ToString());
        AssertBadRequest(DescribeWithEvent, secretName, "<main>: Error: secret `/Root/secret-name` not found\n",
            kikimr, userToken);
    }

    Y_UNIT_TEST_TWIN(GroupGrants, DescribeWithEvent) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);

        const TString secretName = "/Root/secret-name";
        const TString secretValue = "secret-value";
        auto adminSession = kikimr.GetTableClient(NYdb::NTable::TClientSettings().AuthToken("root@builtin"))
            .CreateSession().GetValueSync().GetSession();

        CreateSchemaSecret(secretName, secretValue, adminSession);

        AssertSecretValue(DescribeWithEvent, secretName, secretValue, kikimr, GetUserToken("root@builtin"));

        // assert no grants by default
        const auto userToken = GetUserToken("user@builtin", {"group"});
        AssertBadRequest(DescribeWithEvent, secretName, "<main>: Error: secret `/Root/secret-name` not found\n",
            kikimr, userToken);

        // assert group grants are ok after providing them
        const auto createGroupResult = adminSession.ExecuteSchemeQuery(
            Sprintf("CREATE GROUP `group` WITH USER `user@builtin`;")
        ).GetValueSync();
        UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, createGroupResult.GetStatus(),
            createGroupResult.GetIssues().ToString());

        const auto grantResult = adminSession.ExecuteSchemeQuery(
            Sprintf("GRANT 'ydb.granular.select_row' ON `%s` TO `%s`;", secretName.data(), "group")
        ).GetValueSync();
        UNIT_ASSERT_C(grantResult.GetStatus() == NYdb::EStatus::SUCCESS, grantResult.GetIssues().ToString());

        AssertSecretValue(DescribeWithEvent, secretName, secretValue, kikimr, userToken);

        // assert no grants after revoking them
        const auto revokeResult = adminSession.ExecuteSchemeQuery(
            Sprintf("REVOKE 'ydb.granular.select_row' ON `%s` FROM `%s`;", secretName.data(), "group")
        ).GetValueSync();
        UNIT_ASSERT_EQUAL_C(NYdb::EStatus::SUCCESS, revokeResult.GetStatus(), grantResult.GetIssues().ToString());
        AssertBadRequest(DescribeWithEvent, secretName, "<main>: Error: secret `/Root/secret-name` not found\n",
            kikimr, userToken);
    }

    Y_UNIT_TEST_TWIN(BatchRequest, DescribeWithEvent) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TVector<TString> names;
        TVector<TString> values;
        for (int i = 0; i < 3; ++i) {
            names.push_back("/Root/secret-name-" + ToString(i));
            values.push_back("secret-value-" + ToString(i));
            CreateSchemaSecret(names[i], values[i], session);
        }

        // nothing from cache
        AssertSecretValues(DescribeWithEvent, {names[0], names[1]}, {values[0], values[1]}, kikimr);
        
        // something from cache
        AssertSecretValues(DescribeWithEvent, {names[1], names[2]}, {values[1], values[2]}, kikimr);

        // all from cache
        AssertSecretValues(DescribeWithEvent, names, values, kikimr);
    }

    Y_UNIT_TEST_TWIN(BigBatchRequest, DescribeWithEvent) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        static const int secretsCnt = 10;
        TVector<TString> names;
        TVector<TString> values;
        for (int i = 0; i < secretsCnt; ++i) {
            names.push_back("/Root/secret-name-" + ToString(i));
            values.push_back("secret-value-" + ToString(i));

            CreateSchemaSecret(names[i], values[i], session);
        }

        // nothing from cache [0, batchSize)
        static const auto batchSize = secretsCnt - 3;
        Y_ENSURE(batchSize > 0);
        AssertSecretValues(
            DescribeWithEvent,
            {names.begin(), names.begin() + batchSize},
            {values.begin(), values.begin() + batchSize},
            kikimr
        );

        // something from cache [secretsCnt - batchSize, secretsCnt)
        AssertSecretValues(
            DescribeWithEvent,
            {names.end() - batchSize, names.end()},
            {values.end() - batchSize, values.end()},
            kikimr
        );
    }

    Y_UNIT_TEST_TWIN(EmptyBatch, DescribeWithEvent) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);

        AssertBadRequest(DescribeWithEvent, TVector<TString>{}, "<main>: Error: empty secret names list\n", kikimr);
    }

    Y_UNIT_TEST_TWIN(MixedGrantsInBatch, DescribeWithEvent) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);

        auto adminSession = kikimr.GetTableClient(NYdb::NTable::TClientSettings().AuthToken("root@builtin"))
            .CreateSession().GetValueSync().GetSession();

        TVector<TString> names;
        TVector<TString> values;
        for (int i = 0; i < 2; ++i) {
            names.push_back("/Root/secret-name-" + ToString(i));
            values.push_back("secret-value-" + ToString(i));
            CreateSchemaSecret(names.back(), values.back(), adminSession);
        }

        auto grantResult = adminSession.ExecuteSchemeQuery(
            Sprintf("GRANT 'ydb.granular.select_row' ON `%s` TO `%s`;", names[0].data(), "user@builtin")
        ).GetValueSync();
        UNIT_ASSERT_C(grantResult.GetStatus() == NYdb::EStatus::SUCCESS, grantResult.GetIssues().ToString());

        // user has grants for names[0], has no grants for names[1]
        const auto userToken = GetUserToken("user@builtin");
        AssertBadRequest(DescribeWithEvent, {names[0], names[1]}, "<main>: Error: secret `/Root/secret-name-1` not found\n",
            kikimr, userToken);

        // user has grants for all names[]
        grantResult = adminSession.ExecuteSchemeQuery(
            Sprintf("GRANT 'ydb.granular.select_row' ON `%s` TO `%s`;", names[1].data(), "user@builtin")
        ).GetValueSync();
        UNIT_ASSERT_C(grantResult.GetStatus() == NYdb::EStatus::SUCCESS, grantResult.GetIssues().ToString());

        AssertSecretValues(DescribeWithEvent, names, values, kikimr, userToken);
    }

}

}
