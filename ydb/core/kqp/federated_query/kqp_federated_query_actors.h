#pragma once

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_board/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/aclib/aclib.h>

#include <library/cpp/threading/future/future.h>

namespace NKikimr::NKqp {

enum ESecretEvents {
    EvResolveSecretWithPromise = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
    EvResolveSecretWithEvent,
    EvResolveSecretResponse,
    EvEnd,
};

struct TEvResolveSecretBase {
public:
    TEvResolveSecretBase(
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        const TString& database,
        const TVector<TString>& secretNames
    )
        : UserToken(userToken)
        , Database(database)
        , SecretNames(secretNames)
    {
        Y_ENSURE(!Database.empty(), "Database name must be set in secret requests");
    }

    virtual ~TEvResolveSecretBase() = default;

public:
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString Database;
    const TVector<TString> SecretNames;
};

struct TEvResolveSecretWithPromise
    : public NActors::TEventLocal<TEvResolveSecretWithPromise, EvResolveSecretWithPromise>
    , public TEvResolveSecretBase {
public:
    TEvResolveSecretWithPromise(
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        const TString& database,
        const TVector<TString>& secretNames,
        NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise
    )
        : TEvResolveSecretBase(userToken, database, secretNames)
        , Promise(promise)
    {
    }

public:
    NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> Promise;
};

struct TEvResolveSecretWithEvent
    : public NActors::TEventLocal<TEvResolveSecretWithEvent, EvResolveSecretWithEvent>
    , public TEvResolveSecretBase {
public:
    using TBase = TEvResolveSecretBase;
    TEvResolveSecretWithEvent(
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        const TString& database,
        const TVector<TString>& secretNames
    )
        : TBase(userToken, database, secretNames)
    {
    }
};

struct TEvResolveSecretResponse : public NActors::TEventLocal<TEvResolveSecretResponse, EvResolveSecretResponse> {
    struct TDescription {
        TDescription(Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
            : Status(status)
            , Issues(std::move(issues))
        {}

        TDescription(const std::vector<TString>& secretValues)
            : SecretValues(secretValues)
            , Status(Ydb::StatusIds::SUCCESS)
        {}

        const std::vector<TString> SecretValues;
        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
    };

    TEvResolveSecretResponse(const TDescription& description)
        : Description(description)
    {
    }

    const TDescription Description;
};

class TDescribeSchemaSecretsService: public NActors::TActorBootstrapped<TDescribeSchemaSecretsService> {
private:
    struct TVersionedSecret {
        ui64 SecretVersion = 0;
        ui64 PathId = 0;
        TString Name;
        TString Value;
    };

    struct TSenderActorInfo {
        ui64 Cookie;
        TActorId ActorId;
        TSenderActorInfo(ui64 cookie, const TActorId& actorId)
            : Cookie(cookie)
            , ActorId(actorId)
        {
        }
    };

    struct TResponseContext {
        using TIncomingOrderId = ui64;
        THashMap<TString, TIncomingOrderId> Secrets;
        TMaybe<NThreading::TPromise<TEvDescribeSecretsResponse::TDescription>> Result; // todo rename
        TMaybe<TSenderActorInfo> SenderActorInfo;
        size_t FilledSecretsCnt = 0;
    };

private:
    STRICT_STFUNC(StateWait,
        hFunc(TEvResolveSecretWithPromise, HandleIncomingRequest);
        hFunc(TEvResolveSecretWithEvent, HandleIncomingRequest);
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleSchemeCacheResponse);
        hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, HandleSchemeShardResponse);
        hFunc(TSchemeBoardEvents::TEvNotifyDelete, HandleNotifyDelete);
        hFunc(TSchemeBoardEvents::TEvNotifyUpdate, HandleNotifyUpdate);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    )

    void HandleIncomingRequest(TEvResolveSecretWithPromise::TPtr& ev);
    void HandleIncomingRequest(TEvResolveSecretWithEvent::TPtr& ev);
    void HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void HandleSchemeShardResponse(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev);
    void HandleNotifyDelete(TSchemeBoardEvents::TEvNotifyDelete::TPtr& ev);
    void HandleNotifyUpdate(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev);

    void FillResponse(const ui64& requestId, const TEvDescribeSecretsResponse::TDescription& response);
    void SaveIncomingRequestInfo(const TEvResolveSecretWithPromise& ev);
    void SaveIncomingRequestInfo(const TEvResolveSecretWithEvent& ev, const TActorId& actorId, const ui64 cookie);
    void SendSchemeCacheRequests(const TEvResolveSecretBase& ev);
    bool LocalCacheHasActualVersion(const TVersionedSecret& secret, const ui64& cacheSecretVersion);
    bool LocalCacheHasActualObject(const TVersionedSecret& secret, const ui64& cacheSecretPathId);
    bool HandleSchemeCacheErrorsIfAny(const ui64& requestId, NSchemeCache::TSchemeCacheNavigate& result);
    void FillResponseIfFinished(const ui64& requestId, const TResponseContext& responseCtx);

public:
    TDescribeSchemaSecretsService() = default;

    void Bootstrap();

public:
    // For tests only
    class ISecretUpdateListener : public TThrRefBase {
    public:
        virtual void HandleNotifyDelete(const TString& secretName) = 0;
        virtual ~ISecretUpdateListener() = default;
    };
    void SetSecretUpdateListener(ISecretUpdateListener* secretUpdateListener) {
        SecretUpdateListener = secretUpdateListener;
    }

private:
    ui64 LastCookie = 0;
    THashMap<ui64, TResponseContext> ResolveInFlight;
    THashMap<TString, TVersionedSecret> VersionedSecrets;
    THashMap<TString, TActorId> SchemeBoardSubscribers;
    ISecretUpdateListener* SecretUpdateListener;
};

void RegisterDescribeSecretsActor(
    const NActors::TActorId& replyActorId,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    const std::vector<TString>& secretIds,
    NActors::TActorSystem* actorSystem
);

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(
    const NKikimrSchemeOp::TAuth& authDescription,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    TActorSystem* actorSystem
);

IActor* CreateDescribeSchemaSecretsService();

class IDescribeSchemaSecretsServiceFactory {
public:
    using TPtr = std::shared_ptr<IDescribeSchemaSecretsServiceFactory>;

    virtual IActor* CreateService() = 0;
    virtual ~IDescribeSchemaSecretsServiceFactory() = default;
};

class TDescribeSchemaSecretsServiceFactory : public IDescribeSchemaSecretsServiceFactory {
public:
    IActor* CreateService() override;
};

}  // namespace NKikimr::NKqp
