# Direction: what the first measured answer decides

- Status: draft. The three directions and their criteria are set; the
  thresholds are **not set yet** and are filled in by the owner before the
  first Research Lab result is read.
- Date: 2026-10-20

## Why this exists

MIE may legitimately answer **NO SUITABLE STRATEGY**
([Registry & Suitability](design/BTC_MIE_Strategy_Registry_and_Suitability.md)).
What the project becomes depends on that answer, and deciding it after the
results are in invites the same bias the Research Lab guards against: the
result gets read to fit the preferred outcome. So the decision rule is
written down first, like an experiment definition, and the first complete
pass of milestone 1 ([brief §19](design/BTC_Market_Intelligence_Engine_Research_Brief.md))
is evaluated against it.

Nothing here changes the design. Execution stays out of the MVP (ADR-010),
and LLMs stay research operators (ADR-011).

## The three directions

| | Direction | What MIE is | Partner it needs |
|---|---|---|---|
| **A** | Trade it | An internal tool. The Telegram egress is the operator's own alert channel. Execution would need its own ADR. | None required. At most capital, or a risk and execution partner. |
| **B** | Sell the intelligence | Context, location, trigger, invalidation and the measured outcome distribution behind each alert, delivered to traders. The differentiator is that every claim is measured and the system can say "no". | A trader with an existing audience and trust in the order-flow community. |
| **C** | Sell the research machine | The deterministic data plane, replay, measurement and append-only experiment store, offered to funds, prop desks and quant researchers. | Someone with relationships in crypto funds and market makers. |

If none of the three finds interest, MIE stays a personal research tool.
That is an acceptable outcome, not a failure of the rule.

## The evidence that decides

The evidence comes from Conditional Behavior outputs and from candidates
moving through the Registry lifecycle
(`RESEARCH → BACKTESTED → WALK_FORWARD → PAPER → VALIDATED`). All metrics are
net of fees, slippage and funding, on versioned data and features, per the
Research Lab's anti-overfitting rules.

**A holds when** at least one candidate reaches `VALIDATED` after
walk-forward and the unseen forward test, and:

| Criterion | Threshold |
|---|---|
| Out-of-sample trades (walk-forward + forward test) | to be set |
| Net expectancy and its confidence bound | to be set |
| Net profit factor, walk-forward | to be set |
| Forward test vs walk-forward distribution | to be set |
| Maximum drawdown, out-of-sample | to be set |
| Frequency of suitable setups | to be set |

**B holds when** A does not, or A holds for too few setups to trade, but
conditional behavior shows stable differences that would improve a
discretionary decision:

| Criterion | Threshold |
|---|---|
| Effect size per context against the unconditional baseline | to be set |
| Sample per context, in-sample and out-of-sample | to be set |
| Stability across walk-forward windows and out-of-sample | to be set |
| Counter-hypothesis | The opposite interpretation was tested and lost |
| Number of such contexts and how often they recur | to be set |

**C holds when** neither A nor B does, but the machine itself holds up:
live/replay equivalence passes on the full history chain (ADR-041,
ADR-052), an experiment runs end to end reproducibly from a clean checkout,
and someone outside the project asks to use it within a period after the
results are published (period to be set).

## Rules

1. The thresholds are fixed before the first Research Lab result is read.
   Until then this document may be edited freely.
2. After that, a threshold changes only through a dated addendum at the end
   of this file that states the reason. The original line stays, as in the
   ADR lifecycle.
3. If A and B both hold, A comes first. B is added on top only if
   publishing does not degrade A's execution, for example by crowding the
   same levels.
4. A partner is sought only once a direction holds, with the profile that
   direction names above.
5. Before charging anyone for B's output, check whether it counts as advice
   on crypto-assets under MiCA, which is a licensed service, or as general
   market information.
