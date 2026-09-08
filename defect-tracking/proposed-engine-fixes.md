# CQL engine defect dossier: E-22/E-03 and E-13 — full technical detail for defect-tracking

Shareable, implementation-ready writeup for `dqm-content-cms-2025/defect-tracking/engine-issues.md`
entries **E-22/E-03** and **E-13/E-15**. Both surfaced from measure-content debugging and both
involve USQualityCore/USCore profile types with a `target`/`baseType` relationship to a base FHIR
type — but they are **two independent defects in `clinical_quality_language`'s `cql-to-elm`
translator**, in different code paths, at different compiler phases. This document gives exact
file/line locations, full verbatim code, and concrete fix candidates for each, so implementation
can start directly from this without re-deriving anything.

Source repo: `/Users/raleigh.thompson/projects/smile/vs-code-cql/_repo/clinical_quality_language`
(tag `v5.2.0`, Kotlin Multiplatform: `cql`/`cql-to-elm`/`elm`/`engine` modules).

---

# Issue 1 — E-22/E-03: "Ambiguous call to operator" (ELM-serialization-time bug)

**Symptom**: `MedicationsNotDocumented.recorded()` (CMS68, a `ProcedureNotDone`) throws at
runtime:
```
Ambiguous call to operator 'recorded(org.hl7.elm.r1.NamedTypeSpecifier@...)' in library 'USQualityCoreCommon'.
```
`USQualityCoreCommon.cql` declares both `recorded(procedure Procedure)` and
`recorded(procedureNotDone ProcedureNotDone)` as distinct fluent-function overloads.
`USQualityCore.ProcedureNotDone`'s model-info `classInfo` declares `target="Procedure"`.

## Causal chain (compile time → runtime)

### 1. Model-info ingestion — `target` copied with no uniqueness check

File: `cql-to-elm/src/commonMain/kotlin/org/cqframework/cql/cql2elm/model/ModelImporter.kt`,
`resolveClassType`, lines 591-655 (full method):

```kotlin
private fun resolveClassType(t: ClassInfo): ClassType {
    requireNotNull(t.name) { "Class definition must have a name." }
    val qualifiedName = ensureQualified(t.name!!)
    var result = lookupType(qualifiedName) as ClassType?

    if (result == null) {
        result =
            if (t is ProfileInfo) {
                ProfileType(
                    qualifiedName,
                    resolveTypeNameOrSpecifier(t.baseType, t.baseTypeSpecifier) ?: DataType.ANY,
                )
            } else {
                if (t.name!!.contains("<")) {
                    handleGenericType(t.name!!, t.baseType!!)
                } else {
                    if (t.baseType != null && t.baseType!!.contains("<")) {
                        handleGenericType(t.name!!, t.baseType!!)
                    } else {
                        ClassType(
                            qualifiedName,
                            resolveTypeNameOrSpecifier(t.baseType, t.baseTypeSpecifier)
                                ?: DataType.ANY,
                        )
                    }
                }
            }

        resolvedTypes[casify(result.name)] = result

        result.addGenericParameter(
            resolveGenericParameterDeclarations(t.parameter as List<TypeParameterInfo>)
        )

        result.addElements(
            resolveClassTypeElements(result, t.element as Collection<ClassInfoElement>)
        )

        for (si in t.search) {
            result.addSearch(resolveClassTypeSearch(result, si))
        }

        if (isParentGeneric(result) && !t.baseType!!.contains("<")) {
            validateFreeAndBoundParameters(result, t)
        }

        result.apply {
            identifier = t.identifier
            label = t.label
            target = t.target          // <-- line 648: no uniqueness/collision check
            isRetrievable = t.isRetrievable()!!
            primaryCodePath = t.primaryCodePath
        }
    }

    return result
}
```

Line 648 copies the model-info `target` attribute straight onto `ClassType.target` with **zero
validation** that another `ClassInfo` in the same model doesn't already claim the same `target`.
Note `t.baseType` (a *different* attribute, line ~615) is used to build the real CQL type
hierarchy (`ClassType`'s `baseType` field) — `target` and `baseType` are independent inputs
consumed by disjoint downstream code (this matters for distinguishing Issue 1 from Issue 2).

### 2. ELM serialization — `target` overwrites the type's own name in the QName

File: `cql-to-elm/src/commonMain/kotlin/org/cqframework/cql/cql2elm/TypeBuilder.kt` (full file,
127 lines):

```kotlin
package org.cqframework.cql.cql2elm

import kotlin.collections.ArrayList
import org.cqframework.cql.cql2elm.model.Model
import org.cqframework.cql.cql2elm.tracking.Trackable.withResultType
import org.cqframework.cql.elm.IdObjectFactory
import org.cqframework.cql.shared.QName
import org.hl7.cql.model.*
import org.hl7.elm.r1.ParameterTypeSpecifier
import org.hl7.elm.r1.TupleElementDefinition
import org.hl7.elm.r1.TypeSpecifier
import org.hl7.elm_modelinfo.r1.ModelInfo

class TypeBuilder(private val of: IdObjectFactory, private val mr: ModelResolver) {
    class InternalModelResolver(private val modelManager: ModelManager) : ModelResolver {
        override fun getModel(modelName: String): Model {
            return modelManager.resolveModel(modelName)
        }
    }

    constructor(
        of: IdObjectFactory,
        modelManager: ModelManager,
    ) : this(of, InternalModelResolver(modelManager))

    fun dataTypeToQName(type: DataType?): QName {
        if (type is NamedType) {
            val namedType: NamedType = type
            val modelInfo: ModelInfo = mr.getModel(namedType.namespace).modelInfo
            return QName(
                if (modelInfo.targetUrl != null) modelInfo.targetUrl!! else modelInfo.url!!,
                if (namedType.target != null) namedType.target!! else namedType.simpleName,  // <-- THE BUG
            )
        }
        throw IllegalArgumentException("A named type is required in this context.")
    }

    fun dataTypesToTypeSpecifiers(types: List<DataType>): List<TypeSpecifier> {
        val result: ArrayList<TypeSpecifier> = ArrayList()
        for (type: DataType in types) {
            result.add(dataTypeToTypeSpecifier(type))
        }
        return result
    }

    @Suppress("ReturnCount")
    fun dataTypeToTypeSpecifier(type: DataType?): TypeSpecifier {
        when (type) {
            is NamedType -> {
                return of.createNamedTypeSpecifier()
                    .withName(dataTypeToQName(type))
                    .withResultType(type)
            }
            is ListType -> return listTypeToTypeSpecifier(type)
            is IntervalType -> return intervalTypeToTypeSpecifier(type)
            is TupleType -> return tupleTypeToTypeSpecifier(type)
            is ChoiceType -> return choiceTypeToTypeSpecifier(type)
            is TypeParameter -> return typeParameterToTypeSpecifier(type)
            else -> throw IllegalArgumentException("Could not convert type $type to a type specifier.")
        }
    }
    // ... listTypeToTypeSpecifier / intervalTypeToTypeSpecifier / tupleTypeToTypeSpecifier /
    //     tupleTypeElementsToTupleElementDefinitions / choiceTypeToTypeSpecifier /
    //     choiceTypeTypesToTypeSpecifiers / typeParameterToTypeSpecifier all recurse into
    //     dataTypeToTypeSpecifier -> dataTypeToQName for any NamedType member.
}
```

**This is the bug.** Line `if (namedType.target != null) namedType.target!! else namedType.simpleName`
unconditionally substitutes `target` for the type's own name when building the ELM `QName`. Both
`recorded(procedure Procedure)` and `recorded(procedureNotDone ProcedureNotDone)` end up with
operand `NamedTypeSpecifier.name == QName(namespace, "Procedure")` — textually identical in the
compiled ELM. Note `ClassType.equals`/`hashCode` (`cql/src/commonMain/kotlin/org/hl7/cql/model/ClassType.kt`,
lines 232-242) are keyed on `name` only, so at the real CQL type-system level the two types remain
distinct right up until this exact conversion — the collapse is introduced here, nowhere else.

`dataTypeToQName` is called from every ELM-emission site for a `NamedType`, notably:
- `LibraryBuilder.kt` (`resolveInvocation`, line ~1477-1478): `invocation.signature = dataTypesToTypeSpecifiers(resolution.operator.signature.operandTypes)` — serializes a **resolved call's** signature.
- `SystemLibraryHelper.kt` (`add`, line ~2958): `od.operandType = tb.dataTypeToQName(dataType)` — serializes a **FunctionDef's declared operand type** (this exact pattern is how model-derived function operands, like `recorded`'s two overloads, get their ELM `OperandDef.operandType`).

Full context for the `LibraryBuilder.kt` site (`resolveInvocation`, lines 1428-1505 — included in
full since the `SignatureLevel` handling here is directly relevant to why the "obvious" mitigation
doesn't work):

```kotlin
@JsExport.Ignore
@Suppress("LongParameterList", "LongMethod")
fun resolveInvocation(
    libraryName: String?,
    operatorName: String,
    invocation: Invocation,
    mustResolve: Boolean = true,
    allowPromotionAndDemotion: Boolean = false,
    allowFluent: Boolean = false,
): Invocation? {
    val operands: Iterable<Expression> = invocation.operands
    val callContext = buildCallContext(libraryName, operatorName, operands, mustResolve, allowPromotionAndDemotion, allowFluent)
    val resolution = resolveCall(callContext)
    if (resolution == null && !mustResolve) { return null }
    checkOperator(callContext, resolution)
    val convertedOperands: MutableList<Expression> = ArrayList()
    val operandIterator = operands.iterator()
    val signatureTypes = resolution!!.operator.signature.operandTypes.iterator()
    val conversionIterator = if (resolution.hasConversions()) resolution.conversions.iterator() else null
    while (operandIterator.hasNext()) {
        var operand = operandIterator.next()
        val conversion = conversionIterator?.next()
        if (conversion != null) { operand = convertExpression(operand, conversion) }
        val signatureType = signatureTypes.next()
        operand = pruneChoices(operand, signatureType)
        convertedOperands.add(operand)
    }
    invocation.operands = convertedOperands
    @Suppress("ComplexCondition")
    if (
        options.signatureLevel == SignatureLevel.All ||
            (options.signatureLevel == SignatureLevel.Differing &&
                resolution.operator.signature != callContext.signature) ||
            options.signatureLevel == SignatureLevel.Overloads &&
                resolution.operatorHasOverloads
    ) {
        invocation.signature = dataTypesToTypeSpecifiers(resolution.operator.signature.operandTypes)  // <-- collapsed QName written here too
    } else if (resolution.operatorHasOverloads && resolution.operator.libraryName != "System") {
        // NOTE: Because system functions only deal with CQL system-defined types, and there is
        // one and only one runtime representation of each system-defined type, there is no
        // possibility of ambiguous overload resolution with system functions
        reportWarning(
            """
                The function ${resolution.operator.libraryName}.${resolution.operator.name} has multiple overloads
                and due to the SignatureLevel setting (${options.signatureLevel.name}),
                the overload signature is not being included in the output.
                This may result in ambiguous function resolution
                at runtime, consider setting the SignatureLevel to Overloads or All
                to ensure that the output includes sufficient
                information to support correct overload selection at runtime.
            """.trimIndent().replace("\n", " "),
            invocation.expression,
        )
    }
    invocation.resultType = resolution.operator.resultType
    if (resolution.libraryIdentifier != null) {
        resolution.libraryName = resolveIncludeAlias(resolution.libraryIdentifier!!)
    }
    invocation.resolution = resolution
    return invocation
}
```

**Why `SignatureLevel=Overloads`/`All` does NOT fix this**: that setting only controls *whether*
`invocation.signature` is populated at all. When it is populated, it's built via
`dataTypesToTypeSpecifiers` → `dataTypeToQName` — the exact same collapsing function. So turning
on signatures makes the ambiguity *visible earlier/more consistently* (both call site and function
def carry the same wrong QName), but doesn't disambiguate anything, because the two `Procedure`/
`ProcedureNotDone` overloads' QNames were already identical before this point.

### 3. Runtime dispatch — engine finds two "identical" candidates and throws

File: `engine/src/commonMain/kotlin/org/opencds/cqf/cql/engine/elm/executing/FunctionRefEvaluator.kt`
(full file, 358 lines — key methods below; this is the whole resolution+dispatch object):

```kotlin
object FunctionRefEvaluator {
    // ...
    internal fun resolveFunctionRef(
        state: State?,
        functionRef: FunctionRef,
        arguments: kotlin.collections.List<Value?>,
    ): FunctionDef {
        val name = functionRef.name
        val signature = functionRef.signature
        val functionDefs = resolveFunctionRef(state, name, arguments, signature)
        return pickFunctionDef(state, name, arguments, signature, functionDefs)
    }

    fun resolveFunctionRef(
        state: State?,
        name: String?,
        arguments: kotlin.collections.List<Value?>,
        signature: kotlin.collections.List<TypeSpecifier>,
    ): kotlin.collections.List<FunctionDef> {
        val namedDefs = Libraries.getFunctionDefs(name, state!!.getCurrentLibrary()!!)
        if (!signature.isEmpty()) {
            return namedDefs.filter { x -> functionDefOperandsSignatureEqual(x, signature) }
        }
        logger.debug { "Using runtime function resolution for '$name'. It's recommended to always include signatures in ELM" }
        return namedDefs.filter { x -> matchesTypes(x, arguments, state) }
    }

    fun functionDefOperandsSignatureEqual(
        functionDef: FunctionDef,
        signature: kotlin.collections.List<TypeSpecifier>,
    ): Boolean {
        val operands = functionDef.operand
        return operands.size == signature.size &&
            (0 until operands.size).all { i -> operandDefTypeSpecifierEqual(operands[i], signature[i]) }
    }

    @JvmStatic
    fun operandDefTypeSpecifierEqual(
        operandDef: OperandDef,
        typeSpecifier: TypeSpecifier?,
    ): Boolean {
        val operandDefOperandTypeSpecifier = operandDef.operandTypeSpecifier
        return if (operandDefOperandTypeSpecifier != null) {
            typeSpecifiersEqual(operandDefOperandTypeSpecifier, typeSpecifier)
        } else if (typeSpecifier is NamedTypeSpecifier) {
            qnamesEqual(operandDef.operandType, typeSpecifier.name)   // <-- pure QName equality
        } else false
    }

    fun pickFunctionDef(
        state: State?,
        name: String?,
        arguments: kotlin.collections.List<Value?>,
        signature: kotlin.collections.List<TypeSpecifier>,
        functionDefs: kotlin.collections.List<FunctionDef>,
    ): FunctionDef {
        val types = if (signature.isEmpty()) arguments.joinToString(", ") { it?.typeAsString ?: "null" }
                    else signature.joinToString(", ")
        if (functionDefs.isEmpty()) {
            throw CqlException("Could not resolve call to operator '${name}(${types})' in library '${state!!.getCurrentLibrary()!!.identifier!!.id}'.")
        }
        if (functionDefs.size == 1) {
            return functionDefs[0]
        }
        throw CqlException("Ambiguous call to operator '${name}(${types})' in library '${state!!.getCurrentLibrary()!!.identifier!!.id}'.")  // <-- thrown here
    }
    // ... matchesTypes / isCompatible: only used when signature is EMPTY (runtime-type fallback);
    //     not the path hit here since USQualityCoreCommon compiles with a signature present.
}
```

`operandDefTypeSpecifierEqual` (line ~162) calls `qnamesEqual` — imported from
`elm/src/commonMain/kotlin/org/cqframework/cql/elm/evaluating/SimpleElmEvaluator.kt`:

```kotlin
object SimpleElmEvaluator {
    private val engine = SimpleElmEngine()
    // ...
    @JvmStatic
    fun qnamesEqual(left: QName?, right: QName?): Boolean {
        return engine.qnamesEqual(left, right)
    }
}
```

which delegates to `elm/src/commonMain/kotlin/org/cqframework/cql/elm/evaluating/SimpleElmEngine.kt`:

```kotlin
fun qnamesEqual(left: QName?, right: QName?): Boolean {
    if (left == null && right == null) { return true }
    if (left == null || right == null) { return false }
    return left == right     // <-- plain equality, no specificity/subtype tie-break
}
```

`QName` on the JVM target is a type alias for `javax.xml.namespace.QName`
(`shared/src/jvmMain/kotlin/org/cqframework/cql/shared/QNameJvm.kt`: `actual typealias QName = QName`),
whose documented `equals()` compares only `namespaceURI` + `localPart` (prefix ignored). So once
step 2 has produced two identical `QName(ns, "Procedure")` values for the two overloads'
`OperandDef`s, `qnamesEqual` correctly (per its own contract) reports them equal, `resolveFunctionRef`
returns a list of size 2, and `pickFunctionDef` throws "Ambiguous call."

**The engine's dispatcher is not buggy given its input.** The defect is entirely upstream, in step 2.

## Scope

Any fluent function/operator overloaded across two CQL types that share the same `target` value is
a latent instance of this bug — not limited to `recorded()`/`ProcedureNotDone`/`Procedure`. Worth a
model-info-wide sweep (grep all `classInfo target="..."` values for duplicates within a model) to
size the full blast radius before fixing, since the fix will need to handle however many pairs
exist.

## Fix candidates (for whoever implements)

The core problem: `target` is being used for two different purposes that need to stay separate —
(a) FHIR-path/runtime representation navigation (legitimate use of `target`), and (b) CQL-level
operator/overload-resolution identity (should use the type's own declared name, never `target`).

1. **Narrowest fix**: in `TypeBuilder.dataTypeToQName`, stop substituting `target` when building the
   `QName` used for `OperandDef.operandType`/`FunctionRef.signature` (i.e. for anything that feeds
   overload resolution). Keep `target` substitution only where it's genuinely about runtime
   representation (need to audit: does anything read the ELM `NamedTypeSpecifier.name` produced by
   `dataTypeToQName` to actually locate the FHIR resource type at runtime — e.g. in `Retrieve`
   evaluation or `Property`/path navigation — or is that handled through a completely separate
   mechanism? If `target` is only ever consumed for overload-style dispatch and never for retrieve/
   navigation, `dataTypeToQName` may not need `target` substitution at all, and this could be a
   flat removal of lines `if (namedType.target != null) namedType.target!! else` rather than a
   conditional split — needs verification against how `target` is used elsewhere, e.g. in
   `engine-fhir`'s `FhirModelResolver.resolveType`/`is`, which was noted in earlier investigation to
   resolve FHIR resource types by HAPI's registered class name independent of the model info's
   `target` attribute — suggesting `target` may be *only* consumed by `dataTypeToQName` and nowhere
   else, making removal safe. Confirm before implementing.)
2. **If `target` substitution must be kept for some other legitimate ELM consumer**: give `QName`
   (or a new wrapper) a way to carry both the CQL-declared type identity and its target/representation
   mapping, and update `qnamesEqual`/`operandDefTypeSpecifierEqual`/`functionDefOperandsSignatureEqual`
   to compare on the CQL identity for overload resolution while other consumers keep using the
   target mapping. Bigger change, touches the ELM model's `NamedTypeSpecifier` shape (possibly a
   spec-level change, not just an implementation change) — flag if this is where the trail leads.
3. Add a `ModelImporter`-level or model-info-validation-level check that throws/warns at
   model-compile time if two `ClassInfo`s in the same model share a `target` value and are both
   used as overloaded operand types for the same operator name — a guard rail, not a fix, but would
   at least make this loud at authoring time instead of silent-until-runtime.

Needs design input on which of these to pursue (this was already flagged to the user; they said
"just the diagnosis" for now, so implementation approach is not yet decided — this section exists
so the eventual implementer has candidates to start from, not a mandate to pick one).

---

# Issue 2 — E-13/E-15: "Could not resolve call to operator" (compile-time Choice-widening bug)

**Symptom**: `union`ing `[ConditionEncounterDiagnosis: ...]` and `[ConditionProblemsHealthConcerns: ...]`
produces a `Choice<ConditionEncounterDiagnosis, ConditionProblemsHealthConcerns>`; calling
`.prevalenceInterval()` on it (declared in `FHIRCommon.cql` only as `prevalenceInterval(Condition)`)
fails to compile:
```
Could not resolve call to operator prevalenceInterval with signature (choice<USQualityCore.ConditionEncounterDiagnosis, USQualityCore.ConditionProblemsHealthConcerns>)
```
Both types have model-info `baseType` chains that legitimately resolve to `FHIR.Condition` (traced
and confirmed below) — so this is *not* a broken model-info reference. The bug is in how the
translator's overload resolver handles a `ChoiceType` operand against a single-ancestor-typed
parameter.

## `baseType` chain — traced and confirmed intact

```
USQualityCore.ConditionEncounterDiagnosis        baseType="USCore.ConditionEncounterDiagnosisProfile"
  → USCore.ConditionEncounterDiagnosisProfile     baseType="FHIR.Condition"
    → FHIR.Condition                              baseType="FHIR.DomainResource"   ✔ reaches Condition in 1 hop

USQualityCore.ConditionProblemsHealthConcerns     baseType="USCore.ConditionProblemsHealthConcernsProfile"
  → USCore.ConditionProblemsHealthConcernsProfile baseType="FHIR.Condition"
    → FHIR.Condition                              baseType="FHIR.DomainResource"   ✔ reaches Condition in 1 hop
```

Sources:
- `dqm-content-cms-2025/input/cql/usqualitycore-modelinfo-0.1.0-cibuild.xml:937` (`ConditionEncounterDiagnosis`, `baseType="USCore.ConditionEncounterDiagnosisProfile"`) and `:982` (`ConditionProblemsHealthConcerns`, `baseType="USCore.ConditionProblemsHealthConcernsProfile"`).
- USCore model info (not present in `dqm-content-cms-2025` itself; confirmed via sibling repo copies — `.../us-cql-ig/input/modelinfo/uscore-modelinfo-{7.0.0,8.0.0}.xml`): both `ConditionEncounterDiagnosisProfile` and `ConditionProblemsHealthConcernsProfile` declare `baseType="FHIR.Condition"` directly.
- `FHIR.Condition` confirmed present at `clinical_quality_language/quick/src/main/resources/org/hl7/fhir/fhir-modelinfo-4.0.1.xml:5397`, `baseType="FHIR.DomainResource"`.

**Conclusion: model info is fine. The defect is entirely in `cql-to-elm`'s resolver code.**

## Root cause — `ChoiceType` never widens to a common ancestor of its members

### The type hierarchy machinery, and where it breaks

`ClassType` (individual types) correctly wires `baseType` through to the walkable chain:

File: `cql/src/commonMain/kotlin/org/hl7/cql/model/ClassType.kt`, lines 8-20:
```kotlin
open class ClassType(
    final override val name: String,
    baseType: DataType? = null,
    val elements: MutableList<ClassTypeElement> = mutableListOf(),
    var genericParameters: MutableList<TypeParameter> = mutableListOf(),
) : BaseDataType(baseType), NamedType {
```
— `baseType` is passed through to `BaseDataType(base)`, so `ClassType.baseType` correctly resolves
per the model-info chain above.

`ChoiceType` does **not** do this — it has no `baseType` of its own:

File: `cql/src/commonMain/kotlin/org/hl7/cql/model/ChoiceType.kt`, lines 7-15 (constructor):
```kotlin
data class ChoiceType
private constructor(
    val types: List<DataType>
) : BaseDataType() {   // <-- no `base` argument passed
```
Per `BaseDataType`'s `baseType` getter (`base ?: DataType.ANY`), a `ChoiceType`'s own `.baseType`
is therefore **always `DataType.ANY`**, regardless of what its member types' base types are.
`ChoiceType` also never overrides `isSubTypeOf`/`isSuperTypeOf` — it only defines
`isSubSetOf`/`isSuperSetOf` (lines 28-31), which handle **ChoiceType-vs-ChoiceType** comparisons
only:
```kotlin
// every type in this choice is a subtype of some type in the other choice
fun isSubSetOf(other: ChoiceType): Boolean =
    types.all { x -> other.types.any { x.isSubTypeOf(it) } }
fun isSuperSetOf(other: ChoiceType): Boolean = other.isSubSetOf(this)
```
There is no equivalent for **ChoiceType-vs-plain-ClassType** (e.g. `Choice<A,B>` vs `Condition`).

`BaseDataType.isSuperTypeOf`/`isSubTypeOf` (`cql/src/commonMain/kotlin/org/hl7/cql/model/BaseDataType.kt`,
full file, 61 lines):
```kotlin
abstract class BaseDataType protected constructor(private val base: DataType? = null) : DataType {
    override val baseType
        get() = base ?: DataType.ANY

    override fun isSubTypeOf(other: DataType): Boolean {
        var currentType: DataType = this
        while (currentType != DataType.ANY) {
            if (currentType == other) { return true }
            currentType = currentType.baseType
        }
        return currentType == other
    }

    override fun isSuperTypeOf(other: DataType): Boolean {
        var currentType = other
        while (currentType != DataType.ANY) {
            if (this == currentType) { return true }
            currentType = currentType.baseType
        }
        return this == currentType
    }

    override fun getCommonSuperTypeOf(other: DataType): DataType {
        var currentType: DataType = this
        while (currentType != DataType.ANY) {
            if (currentType.isSuperTypeOf(other)) { return currentType }
            currentType = currentType.baseType
        }
        return DataType.ANY
    }

    override fun isCompatibleWith(other: DataType): Boolean {
        return when (other) {
            this -> true
            is ChoiceType -> other.types.any { this.isSubTypeOf(it) }
            else -> false
        }
    }
}
```
Walking through `FHIR.Condition.isSuperTypeOf(Choice<ConditionEncounterDiagnosis, ConditionProblemsHealthConcerns>)`:
`currentType` starts as the `ChoiceType` instance; `this == currentType` is false (different
classes); `currentType = currentType.baseType` → `DataType.ANY` (per the constructor gap above);
loop exits; returns `this == DataType.ANY` → false. **The member types' individual ancestor chains
are never consulted — the whole-object walk dies on step 1 because `ChoiceType.baseType` is a dead
end.**

### Where this whole-object check is actually invoked during overload resolution

1. `OperatorEntry.kt` (`SignatureNode.resolve`, lines 77-102 relevant excerpt):
```kotlin
// Attempt exact match against this signature
if (operator.signature == invocationSignature) { ... }
// Attempt to resolve against sub signatures
results = subSignatures.resolve(callContext, conversionMap, operatorMap)
// If no subsignatures match, attempt subType match against this signature
if (results.isEmpty() && operator.signature.isSuperTypeOf(invocationSignature)) { ... }
```
`Signature.isSuperTypeOf` (`cql-to-elm/.../model/Signature.kt`, full file):
```kotlin
data class Signature(val operandTypes: List<DataType>) {
    fun isSuperTypeOf(other: Signature): Boolean =
        size == other.size && operandTypes.zip(other.operandTypes).all { it.first.isSuperTypeOf(it.second) }
    fun isSubTypeOf(other: Signature): Boolean =
        size == other.size && operandTypes.zip(other.operandTypes).all { it.first.isSubTypeOf(it.second) }
    fun isConvertibleTo(
        other: Signature, conversionMap: ConversionMap?, operatorMap: OperatorMap,
        allowPromotionAndDemotion: Boolean, conversions: Array<Conversion?>,
    ): Boolean {
        return size == other.size && run {
            for (i in operandTypes.indices) {
                val first = operandTypes[i]; val second = other.operandTypes[i]
                if (first.isSubTypeOf(second)) { continue }
                conversions[i] = conversionMap?.findConversion(first, second, true, allowPromotionAndDemotion, operatorMap)
                if (conversions[i] == null) { return@run false }
            }
            return@run true
        }
    }
}
```
`it.first.isSuperTypeOf(it.second)` / `first.isSubTypeOf(second)` — per-operand, but each call is
the **whole-object** `DataType.isSuperTypeOf`/`isSubTypeOf`, which for a `ChoiceType` operand hits
the dead-end shown above. This is why the plain subtype branch always fails for
`Choice<A,B>` vs `Condition`, falling through to the conversion-map path.

2. `ConversionMap.kt`'s scoring (`getConversionScore`, in the companion object, lines ~407-422):
```kotlin
fun getConversionScore(callOperand: DataType, operand: DataType, conversion: Conversion?): Int {
    return when {
        operand == callOperand -> ConversionScore.ExactMatch.score
        operand.isSuperTypeOf(callOperand) -> ConversionScore.SubType.score   // <-- same dead-end check
        callOperand.isCompatibleWith(operand) -> ConversionScore.Compatible.score
        conversion != null -> conversion.score
        else -> throw IllegalArgumentException("Could not determine conversion score for conversion")
    }
}
```
Same whole-object `isSuperTypeOf` check, same failure mode.

3. The one place that *does* decompose a `Choice` member-by-member —
`ConversionMap.findChoiceConversion` (lines 103-117):
```kotlin
private fun findChoiceConversion(
    fromType: ChoiceType, toType: DataType,
    allowPromotionAndDemotion: Boolean, operatorMap: OperatorMap,
): Conversion? {
    val matches = fromType.types.mapNotNull { choice ->
        findConversion(choice, toType, true, allowPromotionAndDemotion, operatorMap)
    }
    if (matches.isEmpty()) return null
    return Conversion.ChoiceNarrowingCast(fromType, toType, matches.first(), matches.drop(1))
}
```
— is only reached as a fallback inside `findConversion` (`ConversionMap.kt` line ~357,
`fromType is ChoiceType -> findChoiceConversion(...)`), itself only called from
`Signature.isConvertibleTo`'s conversion-map branch, which is only reached *after* the plain
subtype check has already failed for the whole `ChoiceType` object (see #1 above). So in principle
this is the path that *should* rescue `Choice<ConditionEncounterDiagnosis, ConditionProblemsHealthConcerns>`
→ `Condition`, since `findConversion(ConditionEncounterDiagnosis, Condition, ...)` for an
individual member should succeed via ordinary subtype walking. **Whether it actually does succeed,
and if not why, is the one open question this dossier could not fully close** — `findConversion`'s
first branch, `findCompatibleConversion(fromType, toType) ?: internalFindConversion(fromType, toType, isImplicit)`,
needs to be traced to confirm a plain individual-`ClassType`-to-ancestor `ClassType` widening
actually returns a non-null `Conversion` here (as opposed to `internalFindConversion` only
searching *explicitly registered* operator conversions, which a plain inheritance relationship
might not populate) — flagged below as the concrete next investigative step, since it determines
whether the fix belongs in `findChoiceConversion`'s all-must-match strictness (item 2 below) or one
level up, in a missing plain-subtype short-circuit before conversion search is attempted at all
(item 1 below).

Additionally, note `findChoiceConversion`'s own logic is separately defective regardless of the
above: it uses `mapNotNull`+`matches.isEmpty()` (lines 110/114), which only requires **at least
one** choice member to convert, not all of them — silently accepting a `Conversion` built from a
subset of the choice's members. This is a correctness bug independent of whatever's causing the
observed total failure (if it were the only issue, we'd expect the call to *wrongly succeed* using
only one member, not fail outright) — worth fixing either way once the actual failure path is
confirmed.

## Fix candidates (for whoever implements)

1. **Give `ChoiceType`/`BaseDataType`-level operand widening a per-member fallback.** Before (or as
   part of) `Signature.isConvertibleTo`'s per-operand loop, when `first is ChoiceType` and
   `second` is not, check `first.types.all { it.isSubTypeOf(second) }` (i.e. "this choice widens to
   `second` if every member does") and treat that as a supertype/subtype match — not just a
   conversion candidate. This mirrors `isCompatibleWith`'s existing `other is ChoiceType -> other.types.any { this.isSubTypeOf(it) }`
   pattern (`BaseDataType.kt` line ~57) but for the reverse direction and requiring `all` rather
   than `any` (since we want "the whole choice is guaranteed to be a `Condition`", not "the whole
   choice might be one"). Candidate location: either add this as an override in `ChoiceType.kt`
   (`isSubTypeOf`, which it currently doesn't define) delegating to `types.all { it.isSubTypeOf(other) }`,
   or add the check directly in `Signature.isConvertibleTo`/`OperatorEntry.SignatureNode.resolve`
   before falling to conversion search.
2. **Fix `findChoiceConversion`'s partial-match leniency** (`ConversionMap.kt:103-117`) — require
   `matches.size == fromType.types.size` (all members converted), not just `matches.isNotEmpty()`,
   so a "conversion" is never silently built from a subset of the choice.
3. First, **confirm which of the two failure modes is actually occurring** — does
   `findConversion(ConditionEncounterDiagnosis, Condition, ...)` (a plain ancestor-subtype
   situation, no explicit conversion registered) return non-null today? If `internalFindConversion`
   only searches explicitly-registered operator conversions and doesn't fall back to
   `isSubTypeOf`-based success, then *even individual* (non-Choice) cross-hierarchy widening via
   `findConversion` might not work either, and the real gap could be one level higher (item 1) —
   this needs an actual test/trace before choosing between item 1 and item 2, or implementing both.
   Suggested repro: a minimal CQL snippet doing `[ConditionEncounterDiagnosis: "code"] union [ConditionProblemsHealthConcerns: "code"]` piped into a function overloaded only for `Condition`,
   run through `cql-to-elm` with debug logging/breakpoints in `ConversionMap.findConversion` and
   `Signature.isConvertibleTo` to observe which branch is actually taken and where it returns null.

## Cross-reference to prior content-side findings

The existing `engine-issues.md` E-13 entry documents extensive per-measure workarounds (replacing
the `union` with a base `[FHIR.Condition: ...]` retrieve) and confirms via 30-measure regression
testing that the workaround is behaviorally safe. That content-level workaround is a legitimate
deflection and doesn't need to change — this dossier is about the underlying engine defect for
anyone who later wants a real fix instead of the per-measure retrieve rewrite.

---

# Why these are NOT the same defect

| | Issue 1 (E-22/E-03) | Issue 2 (E-13/E-15) |
|---|---|---|
| Error class | `Ambiguous call to operator` (runtime) | `Could not resolve call to operator` (compile-time) |
| Compiler phase | ELM *serialization*, after successful type-check | Semantic *type-checking*/overload resolution, before any ELM is built |
| Code path | `TypeBuilder.dataTypeToQName` → ELM `QName` → `FunctionRefEvaluator.pickFunctionDef` (`qnamesEqual`) | `LibraryBuilder.resolveInvocation` → `OperatorMap`/`OperatorEntry`/`ConversionMap`/`Signature` (`isSubTypeOf`, `findChoiceConversion`) |
| Model-info attribute involved | `target` (consulted only by `dataTypeToQName`) | `baseType` (consulted only by `ClassType.baseType`/`isSubTypeOf` chain) — `target` plays no role |
| Trigger shape | Two sibling types sharing a `target`, both used as overloaded operand types for the *same* function name | A `union`-built `Choice<T1,T2>` passed to a function overloaded only for a common ancestor type |
| Model-info health | N/A (target duplication is legitimate/intentional per USQualityCore's negation-profile pattern) | Confirmed intact — `baseType` chain reaches `FHIR.Condition` cleanly for both types; not a content bug |
| Fix surface | `TypeBuilder.dataTypeToQName` (ELM QName construction) | `ChoiceType`/`Signature`/`ConversionMap` (type-hierarchy widening for Choice operands) |

Both happen to involve USQualityCore/USCore profile types that carry a `target`/`baseType`
relationship, but `target` and `baseType` are independently-populated fields consulted by disjoint
code (confirmed via `ModelImporter.resolveClassType`). Fixing one will not fix the other.

## Recommended framing for defect-tracking

- Keep E-22/E-03 and E-13/E-15 as separate entries but cross-link them with a note that they are
  *not* variants of one root cause (this question came up directly during triage and is worth
  preempting for future readers).
- E-22/E-03: fix candidates are in `TypeBuilder.dataTypeToQName`; needs a decision on which
  candidate (see "Fix candidates" above) before implementation, plus a model-info-wide sweep for
  other `target`-colliding pairs to size full blast radius.
- E-13/E-15: model info is confirmed healthy (baseType chain traced and intact); the fix is in
  `ChoiceType`/`Signature`/`ConversionMap`'s handling of Choice-to-common-ancestor widening. Needs
  one confirming trace (does `findConversion` succeed for a plain non-Choice ancestor widening
  today?) before choosing between the two candidate fix locations.
