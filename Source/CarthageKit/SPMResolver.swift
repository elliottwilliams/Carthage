import Foundation
import PackageGraph
import PackageModel
import ReactiveSwift
import Result
import enum Workspace.ResolverDiagnostics
import struct Basic.AnyError

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

	public func resolve(
		dependencies: [Dependency : VersionSpecifier],
		// where dependenciesToUpdate is nonempty, send these guys along with the updated nodes
		lastResolved: [Dependency : PinnedVersion]?,
		// when nonempty, only send dependencies from the solution that are also in this list
		dependenciesToUpdate: [String]?
	) -> SignalProducer<[Dependency : PinnedVersion], CarthageError> {


		let dependenciesProducer: SignalProducer<[Dependency: VersionSpecifier], CarthageError>
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

	private func constraints(
		for dependencies: SignalProducer<[Dependency: VersionSpecifier], CarthageError>
	) -> SignalProducer<[PackageContainerConstraint], CarthageError> {
		return dependencies
			// Convert the dictionary producer to a key-value producer
			.flatMap(.concat, SignalProducer.init)
			// Convert each Dependency to a DependencyContainer
			.flatMap(.concat) { dependency, versionSpecifier -> SignalProducer<(DependencyContainer, VersionSpecifier), CarthageError> in
				self.container(for: dependency).map { container in (container, versionSpecifier) }
			}
			.map(PackageContainerConstraint.init)
			.collect()
	}

	/// Call SPM's dependency resolver, feeding it information about the dependency graph, and send the package bindings it returns.
	private func solve(
		dependencies: [PackageContainerConstraint],
		pins: [PackageContainerConstraint]
	) -> SignalProducer<DependencyResolver.Binding, CarthageError> {
		let provider = Provider(
			versionsForDependency: versionsForDependency,
			dependenciesForDependency: dependenciesForDependency,
			resolvedGitReference: resolvedGitReference
		)
		// skipUpdate's value doesn't matter, because Carthage always fetches dependency repos before resolving.
		// SPM supports deferring this step to when `Provider.getContainer` is called and allowing it to be
		// selectively disabled.
		let resolver = DependencyResolver(provider, nil, isPrefetchingEnabled: true, skipUpdate: false)

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
			let diagnostics = ResolverDiagnostics.Unsatisfiable(dependencies: unsatisfiableDependencies, pins: unsatisfiablePins)
			// TODO: try to make this an .incompatibleRequirements error. This may be tricky because the resolver
			// doesn't seem to reveal the dependencies that contain the problematic versions.
			return SignalProducer(error: .internalError(description: diagnostics.description))

		case .error(let error):
			return SignalProducer(error: .internalError(description: String(describing: error)))
		}
	}

	/// Sends a `PackageContainer` which wraps the dependency after loading its versions.
	private func container(for dependency: Dependency) -> SignalProducer<DependencyContainer, CarthageError> {
		return DependencyContainer.from(
			dependency: dependency,
			pinnedVersions: self.versionsForDependency(dependency),
			dependenciesForVersion: { pinnedVersion in self.dependenciesForDependency(dependency, pinnedVersion) },
			resolvedGitReference: { revision in self.resolvedGitReference(dependency, revision) }
		)
	}

	/// Convert bound versions from SPM to pinned versions by finding a committish whose parsed version matches the binding.
	func pinnedDependency(
		for identifier: PackageReference,
		boundVersion: BoundVersion
	) -> SignalProducer<(Dependency, PinnedVersion), CarthageError> {
		return SignalProducer(result: Dependency.from(packageReference: identifier).mapError(CarthageError.init))
			.flatMap(.concat) { dependency -> SignalProducer<(Dependency, PinnedVersion), CarthageError> in
				switch boundVersion {
				case .excluded:
					// To be correct, the resolver needs to ensure that this package is _not present_ in the resulting
					// checkout, i.e. it needs not send the dependency even if it's in a preexisting Cartfile. I'm not
					// sure of the circumstances in Carthage that might cause this.
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
}

/// An object queried by SPM to determine available versions and dependencies of some dependency.
private struct Provider: PackageContainerProvider {
	let versionsForDependency: (Dependency) -> SignalProducer<PinnedVersion, CarthageError>
	let dependenciesForDependency: (Dependency, PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>
	let resolvedGitReference: (Dependency, String) -> SignalProducer<PinnedVersion, CarthageError>
	let scheduler = QueueScheduler(
		targeting: DispatchQueue(label: "org.carthage.CarthageKit.SPMResolver.getContainer", attributes: .concurrent)
	)

	func getContainer(for identifier: PackageReference, skipUpdate: Bool, completion: @escaping (SPMResult<PackageContainer, AnyError>) -> Void) {
		SignalProducer(result: Dependency.from(packageReference: identifier))
			.observe(on: self.scheduler)
			.mapError(CarthageError.init)
			.flatMap(.concat) { dependency in
				DependencyContainer.from(
					dependency: dependency,
					pinnedVersions: self.versionsForDependency(dependency),
					dependenciesForVersion: { pinnedVersion in self.dependenciesForDependency(dependency, pinnedVersion) },
					resolvedGitReference: { ref in self.resolvedGitReference(dependency, ref) }
				)
			}
			.map { $0 as PackageContainer }
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


// MARK: - Package container

/// Represents a dependency, its available versions, and their corresponding git references. g
/// facilitates bridging between SPM and Carthage.
private class DependencyContainer {
	let dependency: Dependency
	let versions: [Version]
	let pinnedVersions: [Version: PinnedVersion]

	let dependenciesForVersion: (PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>
	let resolvedGitReference: (String) -> SignalProducer<PinnedVersion, CarthageError>

	var cachedDependenciesForVersion: [Version: [PackageContainerConstraint]] = [:]
	var cachedDependenciesForRevision: [String: [PackageContainerConstraint]] = [:]

	private init(
		dependency: Dependency,
		versions: [Version],
		pinnedVersions: [Version: PinnedVersion],
		dependenciesForVersion: @escaping (PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>,
		resolvedGitReference: @escaping (String) -> SignalProducer<PinnedVersion, CarthageError>
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
		resolvedGitReference: @escaping (String) -> SignalProducer<PinnedVersion, CarthageError>
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
	var identifier: PackageReference {
		return PackageReference.from(dependency: dependency)
	}

	private func constraints(at pinnedVersion: PinnedVersion) -> SignalProducer<PackageContainerConstraint, CarthageError> {
		return dependenciesForVersion(pinnedVersion).map({ dependency, versionSpecifier in
			PackageContainerConstraint(
				container: PackageReference.from(dependency: dependency),
				requirement: PackageRequirement.from(versionSpecifier)
			)
		})
	}

	func versions(filter isIncluded: (Version) -> Bool) -> AnySequence<Version> {
		return AnySequence(versions.filter(isIncluded))
	}

	func getDependencies(at version: Version) throws -> [PackageContainerConstraint] {
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

	func getDependencies(at revision: String) throws -> [PackageContainerConstraint] {
		if let cachedDependencies = cachedDependenciesForRevision[revision] {
			return cachedDependencies
		}

		let dependencies = try resolvedGitReference(revision)
			.flatMap(.concat, constraints(at:)).collect().first()?.get() ?? []
		cachedDependenciesForRevision[revision] = dependencies
		return dependencies
	}

	func getUnversionedDependencies() throws -> [PackageContainerConstraint] {
    // Carthage doesn't have a concept of unversioned dependencies, but this may be called by the constraint debugger.
    return []
	}

	func getUpdatedIdentifier(at boundVersion: BoundVersion) throws -> PackageReference {
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
private let maximumVersion = Version(.max, .max, .max)

private extension PackageRequirement {
	static func from(_ versionSpecifier: VersionSpecifier) -> PackageRequirement {
		switch versionSpecifier {
		case .any:
			return .versionSet(.any)
		case .atLeast(let version):
			return .versionSet(.range(version..<maximumVersion))
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
			if range.upperBound == maximumVersion {
				return .atLeast(range.lowerBound)
			} else {
				return .compatibleWith(range.lowerBound)
			}
		}
	}
}


private extension PackageReference {
	static func from(dependency: Dependency) -> PackageReference {
		return PackageReference(
			identity: dependency.name.lowercased(),
			path: dependency.description,
			name: dependency.name,
			isLocal: false
		)
	}
}

private extension Dependency {
	static func from(packageReference: PackageReference) -> Result<Dependency, ScannableError> {
		let scanner = Scanner(string: packageReference.path)
		return Dependency.from(scanner, base: nil)
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

private extension PackageContainerConstraint {
	init(_ pair: (DependencyContainer, VersionSpecifier)) {
		let (package, pinnedVersion) = pair
		self.init(container: package.identifier, requirement: PackageRequirement.from(pinnedVersion))
	}
}
