import Foundation
import PackageGraph
import PackageModel
import ReactiveSwift
import Result
import enum Workspace.ResolverDiagnostics
import struct Basic.AnyError


extension Dependency: PackageContainerIdentifier {}
private typealias Constraint = PackageContainerConstraint<Dependency>

public struct SPMResolver: ResolverProtocol {
	private typealias VersionedContainers = (dependencies: [(DependencyContainer, VersionSpecifier)], pins: [(DependencyContainer, VersionSpecifier)])

	private let versionsForDependency: (Dependency) -> SignalProducer<PinnedVersion, CarthageError>
	private let dependenciesForDependency: (Dependency, PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>
	private let resolvedGitReference: (Dependency, String) -> SignalProducer<PinnedVersion, CarthageError>

	/// The resolver starts on this queue (although container fetches are performed on the `Provider`'s concurrent scheduler).
	private let resolverQueue = QueueScheduler(name: "org.carthage.CarthageKit.SPMResolver")

	public init(
		versionsForDependency: @escaping (Dependency) -> SignalProducer<PinnedVersion, CarthageError>,
		dependenciesForDependency: @escaping (Dependency, PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>,
		resolvedGitReference: @escaping (Dependency, String) -> SignalProducer<PinnedVersion, CarthageError>
	) {
		self.versionsForDependency = versionsForDependency
		self.dependenciesForDependency = dependenciesForDependency
		self.resolvedGitReference = resolvedGitReference
	}

	// MARK: - Upper half: Interaction with the project

	public func resolve(
		dependencies: [Dependency : VersionSpecifier],
		lastResolved: [Dependency : PinnedVersion]?,
		dependenciesToUpdate: [String]?
	) -> SignalProducer<[Dependency : PinnedVersion], CarthageError> {

		/// Dependencies that the resolver will produce an assignment of.
		let dependenciesProducer: SignalProducer<[Dependency: VersionSpecifier], CarthageError>
		/// Dependencies whose version requirements the resolver takes into consideration, but who don't necessarily
		/// appear in the final assignment.
		///
		/// Pins don't add to the working set of dependencies the resolver is using; they only impose
		/// additional constraints on the dependencies given. This means that pins can contain older resolved
		/// dependencies that should be removed. `dependencies` should contain the Cartfile requirements,
		/// or set given by `Cartfile ∩ (Cartfile.resolved ∪ dependenciesToUpdate)`.
		let pinsProducer: SignalProducer<[Dependency: VersionSpecifier], CarthageError>

		if let lastResolved = lastResolved, let dependenciesToUpdate = dependenciesToUpdate, !dependenciesToUpdate.isEmpty {
			// When performing a partial update, only the dependencies mentioned by `dependenciesToUpdate` or their subdependencies
			// may be updated. Other resolved dependencies should be pinned to their already-resolved versions.

			let dependenciesToUpdate = Set(dependenciesToUpdate)
			let updatableDependencies = dependencies.filter { dependency, _ in
				// Select dependencies which the resolver was explicitly told to update, or which are already part of the resolved set.
				dependenciesToUpdate.contains(dependency.name) || lastResolved[dependency] != nil
			}

			dependenciesProducer = SignalProducer(value: updatableDependencies)
			pinsProducer = pins(for: lastResolved, excluding: dependenciesToUpdate)

		} else {
			// When performing a full update, all dependencies from the Cartfile are taken a face value, and nothing
			// is pinned.
			dependenciesProducer = SignalProducer(value: dependencies)
			pinsProducer = SignalProducer(value: [:])
		}

		// Using package constraints for the Cartfile dependencies and pins based on dependenciesToUpdate...
		return constraints(for: dependenciesProducer).combineLatest(with: constraints(for: pinsProducer))
			// call the solver...
			.observe(on: resolverQueue)
			.flatMap(.concat, solve)
			// convert SPM's binding type into (Dependency, PinnedVersion)...
			.flatMap(.concat, pinnedDependency)
			// and collect into a dictionary.
			.reduce(into: [Dependency: PinnedVersion]()) { resolution, pair in
				let (dependency, pinnedVersion) = pair
				resolution[dependency] = pinnedVersion
		}
	}

	/// Sends a dictionary of dependencies and their specified versions by subtracting dependencies mentioned in
	/// `dependencyNamesToUpdate` from `resolvedDependencies`. The resulting dictionary represents the dependencies
	/// which should be "pinned" and not be changed from their specified versions.
	private func pins(
		for resolvedDependencies: [Dependency: PinnedVersion],
		excluding dependencyNamesToUpdate: Set<String>
	) -> SignalProducer<[Dependency: VersionSpecifier], CarthageError> {
		// Map dependenciesToUpdate into a set of Dependencies
		let dependenciesToUpdate = resolvedDependencies
			.filter { dependency, _ in dependencyNamesToUpdate.contains(dependency.name) }

		/// Sends all nested dependencies of `dependency`, using versions from `resolvedDependencies`.
		func subtree(
			for dependency: Dependency,
			at pinnedVersion: PinnedVersion,
			visited: Set<Dependency>
		) -> SignalProducer<(Dependency, PinnedVersion), CarthageError> {
			print("subtree(for:", dependency, "at:", pinnedVersion, "visited:", visited, ")")
			return self.dependenciesForDependency(dependency, pinnedVersion)
				.filter { dependency, _ in !visited.contains(dependency) }
				.map { dependency, _ in (dependency, resolvedDependencies[dependency]!, visited.union([dependency])) }
				.flatMap(.concat, subtree)
				.concat(SignalProducer(value: (dependency, pinnedVersion)))
		}

		// Add subdependencies to that set, recursively
		let dependenciesToFilterOut = SignalProducer(dependenciesToUpdate)
			.flatMap(.concat) { subtree(for: $0, at: $1, visited: []) }
			.reduce(into: Set<Dependency>()) { set, pair in
				let (dependency, _) = pair
				set.insert(dependency)
		}

		// Subtract the set of all updatable dependencies and subdependencies from resolvedDependencies
		return dependenciesToFilterOut.map { set in
			resolvedDependencies
				.filter { dependency, _ in !set.contains(dependency) }
				// Convert the remaining PinnedVersions into VersionSpecifiers
				.mapValues { pinnedVersion -> VersionSpecifier in
					switch Version.from(pinnedVersion) {
					case .success(let version):
						return .exactly(version)
					case .failure(_):
						return .gitReference(pinnedVersion.commitish)
					}
			}
		}
	}

	// MARK: - Lower half: SPM bridging

	/// Converts dependency requirements to package constraints.
	private func constraints(
		for dependencies: SignalProducer<[Dependency: VersionSpecifier], CarthageError>
	) -> SignalProducer<[Constraint], CarthageError> {
		return dependencies
			// Convert the dictionary producer to a key-value producer
			.flatMap(.concat, SignalProducer.init)
			// Resolve any git references, so that equivalent symbolic git refs will always be given to the resolver as
			// the same object name.
			.flatMap(.concat) { dependency, versionSpecifier -> SignalProducer<(Dependency, VersionSpecifier), CarthageError> in
				if case .gitReference(let revision) = versionSpecifier {
					return self.resolvedGitReference(dependency, revision).map { (dependency, .gitReference($0.commitish)) }
				} else {
					return SignalProducer(value: (dependency, versionSpecifier))
				}
			}
			// Convert each Dependency to a constraint.
			.map { dependency, versionSpecifier -> Constraint in
        Constraint(container: dependency, requirement: PackageRequirement.from(versionSpecifier))
			}
			.collect()
	}

	/// Call SPM's dependency resolver, feeding it information about the dependency graph, and send the package
	/// bindings it returns.
	private func solve(
		dependencies: [Constraint],
		pins: [Constraint]
	) -> SignalProducer<(container: Dependency, binding: BoundVersion), CarthageError> {
		let provider = Provider(
			versionsForDependency: versionsForDependency,
			dependenciesForDependency: dependenciesForDependency,
			resolvedGitReference: resolvedGitReference
		)
		// skipUpdate's value doesn't matter, because Carthage always fetches dependency repos before resolving.
		// SPM supports deferring this step to when `Provider.getContainer` is called and allowing it to be
		// selectively disabled.
		let resolver = DependencyResolver<Provider, NoDelegate<Dependency>>(provider, nil, isPrefetchingEnabled: true, skipUpdate: false)

		// Pins don't add to the add to the working set of dependencies the resolver is using; they only impose
		// additional constraints on the `dependencies` given. This means that `pins` can contain older resolved
		// dependencies that should be removed without issue. `dependencies` should contain the Cartfile requirements,
		// or set given by `Cartfile ∩ (Cartfile.resolved ∪ dependenciesToUpdate)`.
		let result = resolver.resolve(
			dependencies: dependencies,
			pins: pins
		)

		switch result {
		case .success(let bindings):
			return SignalProducer(bindings)

		case .unsatisfiable(let unsatisfiableDependencies, let unsatisfiablePins):
      let requirements = dependencies + pins
      let unsatisfiableRequirements = unsatisfiableDependencies + unsatisfiablePins

      // unsatisfiableRequirements contain any Cartfile requirements that must be removed in order to make the graph
      // resolvable. SPM doesn't provide any information about the graph itself, and unsatisfiableRequirements
      // may be empty if the debugging algorithm timed out.

      func repositoryPackageConstraint(for constraint: Constraint) -> RepositoryPackageConstraint {
        let dependency = constraint.identifier
        return RepositoryPackageConstraint(
          container: PackageReference(identity: dependency.name.lowercased(), path: dependency.relativePath),
          requirement: constraint.requirement
        )
      }

      let diagnostics = ResolverDiagnostics.Unsatisfiable(
        dependencies: unsatisfiableDependencies.map(repositoryPackageConstraint(for:)),
        pins: unsatisfiablePins.map(repositoryPackageConstraint(for:))
      )
      return SignalProducer(error: .internalError(description: diagnostics.description))

		case .error(let error):
			return SignalProducer(error: .internalError(description: String(describing: error)))
		}
	}

	/// Convert bound versions from SPM to pinned versions by finding a committish whose parsed version matches the binding.
	func pinnedDependency(
		for dependency: Dependency,
		boundVersion: BoundVersion
	) -> SignalProducer<(Dependency, PinnedVersion), CarthageError> {
    switch boundVersion {
    case .excluded:
      // To be correct, the resolver needs to ensure that this package is _not present_ in the resulting
      // checkout, i.e. it needs not send the dependency even if it's in a preexisting Cartfile. I'm not
      // aware of any circumstances in Carthage that might cause this.
      fatalError()
    case .revision(let ref):
      return SignalProducer(value: (dependency, PinnedVersion(ref)))
    case .unversioned:
      // Carthage doesn't have unversioned dependencies; every dependency has a version, even if it's just a
      // committish being pointed to.
      fatalError()
    case .version(let version):
      return self.versionsForDependency(dependency)
        .filter { Version.from($0).value == version }
        .take(first: 1)
        .map { (dependency, $0) }
    }
	}
}

/// An object queried by SPM to determine available versions and dependencies of some dependency.
private struct Provider: PackageContainerProvider {
	let versionsForDependency: (Dependency) -> SignalProducer<PinnedVersion, CarthageError>
	let dependenciesForDependency: (Dependency, PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>
	let resolvedGitReference: (Dependency, String) -> SignalProducer<PinnedVersion, CarthageError>
	let scheduler = QueueScheduler(
		targeting: DispatchQueue(label: "org.carthage.CarthageKit.SPMResolver.getContainer", attributes: .concurrent)
	)

	func getContainer(for dependency: Dependency, skipUpdate: Bool, completion: @escaping (SPMResult<DependencyContainer, AnyError>) -> Void) {
    DependencyContainer.from(
      dependency: dependency,
      pinnedVersions: versionsForDependency(dependency),
      dependenciesForVersion: { pinnedVersion in self.dependenciesForDependency(dependency, pinnedVersion) },
      resolvedGitReference: resolvedGitReference
      )
      .observe(on: self.scheduler)
      .startWithResult { result in
        switch result {
        case .success(let container):
          completion(.success(container))
        case .failure(let error):
          completion(.failure(AnyError(error)))
        }
    }
	}
}

/// A DependencyResolverDelegate that does nothing and cannot be instantiated.
private enum NoDelegate<Identifier: PackageContainerIdentifier>: DependencyResolverDelegate { }

// MARK: - Package container

/// Represents a dependency, its available versions, and their corresponding git references. g
/// facilitates bridging between SPM and Carthage.
private class DependencyContainer {
	let dependency: Dependency
	let versions: [Version]
	let pinnedVersions: [Version: PinnedVersion]

	let dependenciesForVersion: (PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>
	let resolvedGitReference: (Dependency, String) -> SignalProducer<PinnedVersion, CarthageError>

	var cachedDependenciesForVersion: [Version: [Constraint]] = [:]
	var cachedDependenciesForRevision: [String: [Constraint]] = [:]

	private init(
		dependency: Dependency,
		versions: [Version],
		pinnedVersions: [Version: PinnedVersion],
		dependenciesForVersion: @escaping (PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>,
		resolvedGitReference: @escaping (Dependency, String) -> SignalProducer<PinnedVersion, CarthageError>
	) {
		self.dependency = dependency
		self.versions = versions
		self.pinnedVersions = pinnedVersions
		self.dependenciesForVersion = dependenciesForVersion
		self.resolvedGitReference = resolvedGitReference
	}

	static func from(
		dependency: Dependency,
		pinnedVersions: SignalProducer<PinnedVersion, CarthageError>,
		dependenciesForVersion: @escaping (PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>,
		resolvedGitReference: @escaping (Dependency, String) -> SignalProducer<PinnedVersion, CarthageError>
	) -> SignalProducer<DependencyContainer, CarthageError> {

		return pinnedVersions
			.filterMap { pinnedVersion in
				Version.from(pinnedVersion)
					.map { ($0, pinnedVersion) }
					.value
			}
			.reduce(into: [Version: PinnedVersion]()) { pinnedVersionForVersion, pair in
				let (version, pinnedVersion) = pair
				pinnedVersionForVersion[version] = pinnedVersion
			}
			.map { pinnedVersionForVersion in
				return DependencyContainer(
					dependency: dependency,
					versions: pinnedVersionForVersion.keys.sorted(by: { $0 >= $1 }),
					pinnedVersions: pinnedVersionForVersion,
					dependenciesForVersion: dependenciesForVersion,
					resolvedGitReference: resolvedGitReference
				)
			}
	}
}

extension DependencyContainer: PackageContainer {
	var identifier: Dependency {
		return dependency
	}

	private func constraints(at pinnedVersion: PinnedVersion) -> SignalProducer<Constraint, CarthageError> {
		return dependenciesForVersion(pinnedVersion)
			.flatMap(.concat) { dependency, versionSpecifier -> SignalProducer<(Dependency, VersionSpecifier), CarthageError> in
					if case .gitReference(let revision) = versionSpecifier {
						return self.resolvedGitReference(dependency, revision)
							.map { (dependency, .gitReference($0.commitish)) }
					} else {
						return SignalProducer(value: (dependency, versionSpecifier))
					}
			}
			.map({ dependency, versionSpecifier in
				Constraint(
					container: dependency,
					requirement: PackageRequirement.from(versionSpecifier)
				)
			})
	}

	func versions(filter isIncluded: (Version) -> Bool) -> AnySequence<Version> {
		return AnySequence(versions.filter(isIncluded))
	}

	func getDependencies(at version: Version) throws -> [Constraint] {
		if let cachedDependencies = cachedDependenciesForVersion[version] {
			return cachedDependencies
		}

		guard let pinnedVersion = pinnedVersions[version] else {
			preconditionFailure()
		}

		let dependencies = try constraints(at: pinnedVersion).collect().first()?.get() ?? []
		cachedDependenciesForVersion[version] = dependencies
		return dependencies
	}

	func getDependencies(at revision: String) throws -> [Constraint] {
		if let cachedDependencies = cachedDependenciesForRevision[revision] {
			return cachedDependencies
		}

		let dependencies = try resolvedGitReference(dependency, revision)
			.flatMap(.concat, constraints(at:)).collect().first()?.get() ?? []
		cachedDependenciesForRevision[revision] = dependencies
		return dependencies
	}

	func getUnversionedDependencies() throws -> [Constraint] {
    // Carthage doesn't have a concept of unversioned dependencies, but this may be called by the constraint debugger.
    return []
	}

	func getUpdatedIdentifier(at boundVersion: BoundVersion) throws -> Dependency {
		return identifier
	}
}

extension DependencyContainer: Hashable {
	static func == (lhs: DependencyContainer, rhs: DependencyContainer) -> Bool {
		return lhs.dependency == rhs.dependency && lhs.versions == rhs.versions
	}

	func hash(into hasher: inout Hasher) {
		hasher.combine(dependency)
		hasher.combine(versions)
	}
}


// MARK: - Conversions

// TODO: emw: Is there a better way to determine the upper bound? Almost certainly.
private extension Version {
	/// The largest expressible version. Used to convert Carthage's `atLeast` version specifier to SPM's `versionSet`.
	static var max: Version {
		return Version(.max, .max, .max)
	}
}

private extension PackageRequirement {
	static func from(_ versionSpecifier: VersionSpecifier) -> PackageRequirement {
		switch versionSpecifier {
		case .any:
			return .versionSet(.any)
		case .atLeast(let version):
			return .versionSet(.range(version..<Version.max))
		case .compatibleWith(let version):
			let nextMajor = Version(version.major + 1, 0, 0)
			return .versionSet(.range(version..<nextMajor))
		case .exactly(let version):
			return .versionSet(.exact(version))
		case .gitReference(let ref):
			return .revision(ref)
		}
	}
}

private extension VersionSpecifier {
	static func from(_ packageRequirement: PackageRequirement) -> VersionSpecifier {
		switch packageRequirement {
		case .revision(let revision):
			return .gitReference(revision)
		case .unversioned,
				 .versionSet(.any):
			return .any
		case .versionSet(.empty):
			fatalError()
		case .versionSet(.exact(let version)):
			return .exactly(version)
		case .versionSet(.range((let range))):
			if range.upperBound == Version.max {
				return .atLeast(range.lowerBound)
			} else {
				return .compatibleWith(range.lowerBound)
			}
		}
	}
}

extension SPMResult {
	static func from(result: Result<Value, ErrorType>) -> SPMResult<Value, ErrorType> {
		switch result {
		case .failure(let error):
			return .failure(error)
		case .success(let success):
			return .success(success)
		}
	}
}
