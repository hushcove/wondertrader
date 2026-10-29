// Linux offline regression tests against the real IDataWriter implementation.
#include <sys/wait.h>
#include <unistd.h>
#include <boost/filesystem.hpp>
#include <cmath>
#include <fstream>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <vector>
#include "../Includes/IBaseDataMgr.h"
#include "../Includes/IDataWriter.h"
#include "../Includes/WTSSessionInfo.hpp"
#include "../Includes/WTSContractInfo.hpp"
#include "../Includes/WTSDataDef.hpp"
#include "../Includes/WTSVariant.hpp"
#include "../WtDataStorage/DataDefine.h"

using namespace wtp;
extern "C" IDataWriter* createWriter();
static unsigned passedCases = 0;

static void require(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}
static void equal(double actual, double expected, const char* name) {
    require(std::fabs(actual - expected) < 0.00001,
        std::string(name) + ": expected " + std::to_string(expected) + ", got " + std::to_string(actual));
}

struct BaseData : IBaseDataMgr {
    WTSSessionInfo* session;
    WTSCommodityInfo* commodity;
    WTSContractInfo* contract;
    BaseData() {
        session = WTSSessionInfo::create("TEST", "TEST", 300);
        session->setAuctionTime(2059, 2100);
        session->addTradingSection(2100, 230);
        session->addTradingSection(900, 1015);
        session->addTradingSection(1030, 1130);
        session->addTradingSection(1330, 1500);
        commodity = WTSCommodityInfo::create("rb", "test", "SHFE", "TEST", "CHINA");
        commodity->setSessionInfo(session);
        commodity->setVolScale(10);
        commodity->setPriceTick(1);
        commodity->setPriceMode((PriceMode)0);
        contract = WTSContractInfo::create("rb2701", "test", "SHFE", "rb");
        contract->setCommInfo(commodity);
    }
    WTSCommodityInfo* getCommodity(const char*) override { return commodity; }
    WTSCommodityInfo* getCommodity(const char*, const char*) override { return commodity; }
    WTSContractInfo* getContract(const char*, const char*, uint32_t) override { return contract; }
    WTSArray* getContracts(const char*, uint32_t) override { return nullptr; }
    WTSSessionInfo* getSession(const char*) override { return session; }
    WTSSessionInfo* getSessionByCode(const char*, const char*) override { return session; }
    WTSArray* getAllSessions() override { return nullptr; }
    bool isHoliday(const char*, uint32_t, bool) override { return false; }
    uint32_t calcTradingDate(const char*, uint32_t date, uint32_t, bool) override { return date; }
    uint64_t getBoundaryTime(const char*, uint32_t, bool, bool) override { return 0; }
    uint32_t getContractSize(const char*, uint32_t) override { return 1; }
};

struct Sink : IDataWriterSink {
    BaseData base;
    std::vector<std::string> logs;
    IBaseDataMgr* getBDMgr() override { return &base; }
    bool canSessionReceive(const char*) override { return true; }
    void broadcastTick(WTSTickData*) override {}
    void broadcastOrdQue(WTSOrdQueData*) override {}
    void broadcastOrdDtl(WTSOrdDtlData*) override {}
    void broadcastTrans(WTSTransData*) override {}
    CodeSet* getSessionComms(const char*) override { return nullptr; }
    uint32_t getTradingDate(const char*) override { return 20261029; }
    void outputLog(WTSLogLevel, const char* message) override { logs.emplace_back(message); }
    bool logged(const std::string& message) const {
        for (const auto& line : logs) if (line.find(message) != std::string::npos) return true;
        return false;
    }
};

struct Fixture {
    Sink sink;
    IDataWriter* writer;
    std::string folder;
    Fixture(const std::string& path, bool reset = true, uint32_t gap = 60) : folder(path) {
        boost::filesystem::create_directories(folder);
        auto cfg = WTSVariant::createObject();
        cfg->append("path", folder.c_str());
        cfg->append("async", false);
        cfg->append("groupsize", (uint32_t)1000);
        cfg->append("disabletick", true);
        cfg->append("disablemin5", true);
        cfg->append("disableday", true);
        cfg->append("disablehis", true);
        cfg->append("reset_cumulative_on_start", reset);
        cfg->append("cumulative_gap_seconds", gap);
        writer = createWriter();
        require(writer && writer->init(cfg, &sink), "writer init failed");
        cfg->release();
    }
    void tick(uint32_t time, double volume, double money, double interest,
              uint32_t date = 20261029, uint32_t tradingDay = 20261029, uint32_t flag = 1) {
        auto tick = WTSTickData::create("rb2701");
        tick->setContractInfo(sink.base.contract);
        auto& data = tick->getTickStruct();
        strcpy(data.exchg, "SHFE");
        data.trading_date = tradingDay;
        data.action_date = date;
        data.action_time = time;
        data.price = 3059;
        data.total_volume = volume;
        data.total_turnover = money;
        data.open_interest = interest;
        data.pre_interest = 1000;
        if (!flag) {
            data.volume = volume;
            data.turn_over = money;
            data.diff_interest = interest - 1000;
        }
        require(writer->writeTick(tick, flag), "writeTick rejected");
        tick->release();
    }
    std::vector<WTSBarStruct> bars() {
        std::ifstream input(folder + "/rt/min1/SHFE/rb2701.dmb", std::ios::binary);
        require(input.good(), "minute cache missing");
        RTDayBlockHeader header;
        input.read((char*)&header, sizeof(header));
        require(input.good() && header._type == BT_RT_Minute1 && header._size <= header._capacity,
                "invalid minute-cache header");
        std::vector<WTSBarStruct> result(header._size);
        input.read((char*)result.data(), result.size() * sizeof(WTSBarStruct));
        require(input.good(), "minute cache truncated");
        return result;
    }
};

// Each writer runs in its own process. _Exit deliberately avoids the existing,
// unrelated upstream thread-teardown bug; these tests do not certify shutdown.
static void isolated(const std::string& name, const std::function<void()>& body) {
    pid_t pid = fork();
    require(pid >= 0, "fork failed");
    if (pid == 0) {
        try { body(); std::_Exit(0); }
        catch (const std::exception& ex) { std::cerr << name << ": " << ex.what() << std::endl; std::_Exit(1); }
    }
    int status = 0;
    require(waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0,
            name + " failed");
    std::cout << "PASS " << name << std::endl;
    ++passedCases;
}

int main(int argc, char** argv) {
    try {
        require(argc == 2, "usage: TestDataWriter <new-scratch-directory>");
        const std::string root = argv[1];
        require(!boost::filesystem::exists(root), "scratch directory must not already exist");
        boost::filesystem::create_directories(root);
        isolated("legacy default compatibility", [&] {
            Fixture f(root + "/legacy", false);
            f.tick(134103000, 636557, 1000000, 1100);
            equal(f.bars().at(0).vol, 636557, "legacy volume");
        });
        isolated("midday cold start and subsequent increments", [&] {
            Fixture f(root + "/cold");
            f.tick(134103000, 636557, 1000000, 1100);
            auto first = f.bars().at(0);
            equal(first.vol, 0, "initial volume"); equal(first.money, 0, "initial turnover"); equal(first.add, 0, "initial oi change");
            equal(first.open, 3059, "price retained"); equal(first.hold, 1100, "absolute interest retained");
            f.tick(134159000, 637249, 1006920, 1103);
            auto bar = f.bars().at(0);
            equal(bar.vol, 692, "observed first-minute volume"); equal(bar.money, 6920, "observed turnover"); equal(bar.add, 3, "observed oi change");
            require(f.sink.logged("current minute is partial"), "missing partial-minute warning");
            f.tick(134200000, 637254, 1006970, 1105);
            equal(f.bars().at(1).vol, 5, "next minute normal delta");
        });
        isolated("opening minute retains auction volume", [&] {
            Fixture f(root + "/open");
            f.tick(210000000, 10, 1000, 1005, 20261028);
            f.tick(210001000, 13, 1300, 1007, 20261028);
            equal(f.bars().at(0).vol, 13, "opening volume"); equal(f.bars().at(0).money, 1300, "opening turnover");
        });
        isolated("opening auction retains cumulative snapshot", [&] {
            Fixture f(root + "/auction");
            f.tick(205930000, 7, 700, 1002, 20261028);
            equal(f.bars().at(0).vol, 7, "auction volume");
        });
        isolated("seed persisted cache", [&] {
            Fixture f(root + "/restart");
            f.tick(134103000, 100, 1000, 1100);
            f.tick(134110000, 105, 1050, 1101);
        });
        isolated("same-minute restart preserves exact delta", [&] {
            Fixture f(root + "/restart");
            f.tick(134120000, 108, 1080, 1102);
            equal(f.bars().at(0).vol, 8, "persisted volume plus same-minute delta");
        });
        isolated("cross-minute restart establishes fresh baseline", [&] {
            Fixture f(root + "/restart");
            f.tick(134430000, 500, 5000, 1200);
            equal(f.bars().at(1).vol, 0, "unallocated downtime volume");
            f.tick(134431000, 503, 5030, 1202);
            equal(f.bars().at(1).vol, 3, "post-restart delta");
            require(f.sink.logged("Cumulative recovery gap"), "missing restart-gap warning");
        });
        isolated("live quote gap does not inflate recovery minute", [&] {
            Fixture f(root + "/gap");
            f.tick(134103000, 100, 1000, 1100);
            f.tick(134110000, 105, 1050, 1101);
            f.tick(134430000, 500, 5000, 1200);
            equal(f.bars().at(1).vol, 0, "gap volume"); equal(f.bars().at(1).money, 0, "gap turnover");
            require(f.sink.logged("unallocated volume 395"), "missing unallocated-volume quality record");
        });
        isolated("gap reset can be disabled independently", [&] {
            Fixture f(root + "/gap-off", true, 0);
            f.tick(134103000, 100, 1000, 1100);
            f.tick(134430000, 105, 1050, 1101);
            equal(f.bars().at(1).vol, 5, "gap reset disabled");
        });
        isolated("new trading day after midday uses new baseline", [&] {
            Fixture f(root + "/new-day");
            f.tick(134103000, 100, 1000, 1100);
            f.tick(134103000, 500, 5000, 1200, 20261030, 20261030);
            equal(f.bars().back().vol, 0, "new-day midday volume");
        });
        isolated("midnight within one trading day keeps normal delta", [&] {
            Fixture f(root + "/midnight");
            f.tick(235959000, 100, 1000, 1100, 20261028);
            f.tick(1000, 107, 1070, 1103);
            equal(f.bars().back().vol, 7, "midnight delta");
        });
        isolated("preprocessed ticks remain unchanged", [&] {
            Fixture f(root + "/preprocessed");
            f.tick(134103000, 7, 70, 1002, 20261029, 20261029, 0);
            equal(f.bars().at(0).vol, 7, "preprocessed volume");
        });
        isolated("scheduled lunch break preserves normal increment", [&] {
            Fixture f(root + "/lunch");
            f.tick(113000000, 100, 1000, 1100);
            f.tick(133000000, 108, 1080, 1103);
            equal(f.bars().back().vol, 8, "lunch reopening volume");
            require(!f.sink.logged("Cumulative recovery gap"), "lunch misclassified as outage");
        });
        isolated("seed cache at session close", [&] {
            Fixture f(root + "/lunch-restart");
            f.tick(113000000, 100, 1000, 1100);
        });
        isolated("restart across scheduled break preserves reopening", [&] {
            Fixture f(root + "/lunch-restart");
            f.tick(133000000, 108, 1080, 1103);
            equal(f.bars().back().vol, 8, "cached lunch reopening volume");
        });
        isolated("day-only opening retains initial trades", [&] {
            Fixture f(root + "/day-open");
            auto session = WTSSessionInfo::create("DAY", "DAY");
            session->addTradingSection(900, 1130);
            session->addTradingSection(1330, 1500);
            f.sink.base.session = session;
            f.sink.base.commodity->setSessionInfo(session);
            f.tick(90000000, 12, 1200, 1004);
            equal(f.bars().at(0).vol, 12, "day opening trades");
        });
        isolated("new trading day opening keeps initial volume", [&] {
            Fixture f(root + "/rollover");
            f.tick(134103000, 100, 1000, 1100);
            f.tick(210000000, 10, 100, 1003, 20261029, 20261030);
            equal(f.bars().back().vol, 10, "new trading day opening volume");
        });
        isolated("minimal snapshot mode keeps legacy gap behavior", [&] {
            Fixture f(root + "/minimal");
            f.tick(134103000, 100, 1000, 1100, 20261029, 20261029, 2);
            f.tick(134110000, 105, 1050, 1101, 20261029, 20261029, 2);
            f.tick(134430000, 500, 5000, 1200, 20261029, 20261029, 2);
            equal(f.bars().back().vol, 395, "non-cumulative-mode compatibility");
            require(!f.sink.logged("Cumulative recovery gap"), "policy applied outside procFlag 1");
        });
        std::cout << "All " << passedCases << " DataWriter regression cases passed." << std::endl;
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
        return 1;
    }
}
