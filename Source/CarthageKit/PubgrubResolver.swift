import Foundation
import PackageGraph
import ReactiveSwift
import Result

import PackageModel
import struct Basic.AnyError

public struct PubgrubResolver: ResolverProtocol {

  private let versionsForDependency: (Dependency) -> SignalProducer<PinnedVersion, CarthageError>
  private let dependenciesForDependency: (Dependency, PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>
  private let resolvedGitReference: (Dependency, String) -> SignalProducer<PinnedVersion, CarthageError>
	private let scheduler = QueueScheduler(
		targeting: DispatchQueue(label: "org.carthage.CarthageKit.PubgrubResolver.getContainer", attributes: .concurrent)
	)

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

		return SignalProducer(dependencies)
			.flatMap(.merge) { dependency, versionSpecifier -> SignalProducer<(DependencyContainer, VersionSpecifier), CarthageError> in
				DependencyContainer.from(
					dependency: dependency,
					pinnedVersions: self.versionsForDependency(dependency),
					dependenciesForVersion: { pinnedVersion in self.dependenciesForDependency(dependency, pinnedVersion) },
					resolvedGitReference: { revision in self.resolvedGitReference(dependency, revision) }
					).map { container in
						(container, versionSpecifier)
				}
			}
			.collect()
			.flatMap(.concat) { dependencyVersionPairs -> SignalProducer<[Dependency : PinnedVersion], CarthageError> in
				let loadedDependencies = Dictionary(uniqueKeysWithValues: dependencyVersionPairs)
				return self.solve(dependencies: loadedDependencies)
					.collect()
					.map(Dictionary.init(uniqueKeysWithValues:))
				}
		// TODO: additional filtering using `lastResolved` and `dependenciesToUpdate`
	}

	private func solve(dependencies: [DependencyContainer: VersionSpecifier]) -> SignalProducer<(Dependency, PinnedVersion), CarthageError> {

		let project = ProjectContainer(dependencies: dependencies)
		let provider = Provider(
			project: ProjectContainer(dependencies: dependencies),
			versionsForDependency: versionsForDependency,
			dependenciesForDependency: dependenciesForDependency,
			resolvedGitReference: resolvedGitReference
		)
		let resolver = PubgrubDependencyResolver(provider, nil, isPrefetchingEnabled: true, skipUpdate: false/* dependenciesToUpdate != nil */)

		let result = resolver.solve(root: project.identifier, pins: try! project.getUnversionedDependencies())

		func pinnedDependency(for identifier: PackageReference, boundVersion: BoundVersion) -> SignalProducer<(Dependency, PinnedVersion), CarthageError> {
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

		switch result {
		case .success(let bindings):
			return SignalProducer(bindings).flatMap(.concat, pinnedDependency)

		case .unsatisfiable(let unsatisfiableDependencies, let unsatisfiablePins):
			guard let identifier = unsatisfiableDependencies.first?.identifier else {
					preconditionFailure()
			}

			let dependency = SignalProducer(result: Dependency.from(packageReference: identifier))

			let pins = SignalProducer(unsatisfiablePins)
				.filter { $0.identifier == identifier }
				.promoteError(ScannableError.self)
				.attemptMap { (constraint: PackageContainerConstraint) -> Result<CarthageError.VersionRequirement, ScannableError> in
					Dependency.from(packageReference: constraint.identifier)
						.map { dependency in
							(specifier: VersionSpecifier.from(constraint.requirement), fromDependency: dependency)
						}
				}
				.collect(count: 2)

			return dependency.zip(with: pins)
				.mapError(CarthageError.init)
				.flatMap(.concat) { dependency, pinnedRequirements in
					SignalProducer(error: .incompatibleRequirements(dependency, pinnedRequirements[0], pinnedRequirements[1]))
				}

		case .error(let error):
			return SignalProducer(error: .internalError(description: error.localizedDescription))
		}
	}
}

/// An object queried by SPM to determine available versions and dependencies of some dependency.
private struct Provider: PackageContainerProvider {
	let project: ProjectContainer
	let versionsForDependency: (Dependency) -> SignalProducer<PinnedVersion, CarthageError>
	let dependenciesForDependency: (Dependency, PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>
	let resolvedGitReference: (Dependency, String) -> SignalProducer<PinnedVersion, CarthageError>
	let scheduler = QueueScheduler(
		targeting: DispatchQueue(label: "org.carthage.CarthageKit.PubgrubResolver.getContainer", attributes: .concurrent)
	)

	func getContainer(for identifier: PackageReference, skipUpdate: Bool, completion: @escaping (SPMResult<PackageContainer, AnyError>) -> Void) {
		let getProject = SignalProducer(value: project as PackageContainer).promoteError(CarthageError.self)
		let getDependency = SignalProducer(result: Dependency.from(packageReference: identifier))
			.mapError(CarthageError.init)
			.observe(on: scheduler)
			.flatMap(.concat) { dependency in
				DependencyContainer.from(
					dependency: dependency,
					pinnedVersions: self.versionsForDependency(dependency),
					dependenciesForVersion: { pinnedVersion in self.dependenciesForDependency(dependency, pinnedVersion) },
					resolvedGitReference: { ref in self.resolvedGitReference(dependency, ref) }
				)
			}
			.map { $0 as PackageContainer }

		let producer = identifier == project.identifier ? getProject : getDependency
		producer.startWithResult { result in
			switch result {
			case .success(let container):
				completion(.success(container))
			case .failure(let error):
				completion(.failure(AnyError(error)))
			}
		}
	}
}

#if false
private class EncounteredDependencies {
	let dependenciesForDependency: (Dependency, PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>
	var cache: [Dependency: VersionSpecifier] = [:]

	init(dependenciesForDependency: @escaping (Dependency, PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>) {
		self.dependenciesForDependency = dependenciesForDependency
	}

	func dependenciesForVersion(of dependency: Dependency) -> (PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError> {
		return { pinnedVersion in
			self.dependenciesForDependency(dependency, pinnedVersion)
				.on(value: { cache[$0] = $1 })
		}
	}

	subscript(identifier: PackageReference) -> PackageContainer? {
		get { return dependencies[identifier] }
		set { dependencies[identifier] = newValue }
	}
}
#endif


// MARK: - Package container

/// Represents a dependency, its available versions, and their corresponding git references. g
/// facilitates bridging between SPM and Carthage.
private struct DependencyContainer {
  let dependency: Dependency
  let versions: [PinnedVersion: Version]
  let pinnedVersions: [Version: PinnedVersion]

  let dependenciesForVersion: (PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>
  let resolvedGitReference: (String) -> SignalProducer<PinnedVersion, CarthageError>

  static func from(
    dependency: Dependency,
    pinnedVersions: SignalProducer<PinnedVersion, CarthageError>,
    dependenciesForVersion: @escaping (PinnedVersion) -> SignalProducer<(Dependency, VersionSpecifier), CarthageError>,
    resolvedGitReference: @escaping (String) -> SignalProducer<PinnedVersion, CarthageError>
  ) -> SignalProducer<DependencyContainer, CarthageError> {

		return pinnedVersions
			.attemptMap { pinnedVersion in
				Version.from(pinnedVersion)
					.map { (pinnedVersion, $0) }
					.mapError(CarthageError.init)
			}
			.collect()
			.map { pairedVersions in
				let reversedVersions = pairedVersions.map { ($1, $0) }
				return DependencyContainer(
					dependency: dependency,
					versions: Dictionary(uniqueKeysWithValues: pairedVersions),
					pinnedVersions: Dictionary(uniqueKeysWithValues: reversedVersions),
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
    return AnySequence(versions.values.filter(isIncluded))
  }

  func getDependencies(at version: Version) throws -> [PackageContainerConstraint] {
    guard let pinnedVersion = pinnedVersions[version] else {
      preconditionFailure()
    }

    return try constraints(at: pinnedVersion)
			.collect().first()?.get() ?? []
  }

	func getDependencies(at revision: String) throws -> [PackageContainerConstraint] {
		return try resolvedGitReference(revision).flatMap(.concat, constraints(at:))
			.collect().first()?.get() ?? []
	}

  func getUnversionedDependencies() throws -> [PackageContainerConstraint] {
    fatalError("All dependencies are versioned")
  }

  func getUpdatedIdentifier(at boundVersion: BoundVersion) throws -> PackageReference {
    return identifier
  }
}

private struct ProjectContainer: PackageContainer {
	let dependencies: [DependencyContainer: VersionSpecifier]

	static var sharedIdentifier: PackageReference {
		return PackageReference(identity: "project", path: ".")
	}

	var identifier: PackageReference {
		return ProjectContainer.sharedIdentifier
	}

	func versions(filter isIncluded: (Version) -> Bool) -> AnySequence<Version> {
		return AnySequence([Version(0, 0, 0)])
	}

	func getDependencies(at version: Version) throws -> [PackageContainerConstraint] {
		fatalError("The ProjectContainer should always be unversioned")
	}

	func getDependencies(at revision: String) throws -> [PackageContainerConstraint] {
		fatalError("The ProjectContainer should always be unversioned")
	}

	func getUnversionedDependencies() throws -> [PackageContainerConstraint] {
		return dependencies.map { container, versionSpecifier in
			PackageContainerConstraint(
				container: container.identifier,
				requirement: PackageRequirement.from(versionSpecifier)
			)
		}
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
